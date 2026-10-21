"""One-replica, bounded Kafka consumer with durable batch retry and WIF auth."""
import asyncio
import hashlib
import json
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.structs import OffsetAndMetadata

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from export_lms_kafka import TOPIC, normalize_source
from worker.bigquery_sink import BigQuerySink
from worker.state import Journal

STATE = {'ready':False,'last_loop':0.0,'last_ack':None,'acknowledged_rows':0,
         'successful_batches':0,'last_error_class':None,'pending_records':0,
         'assigned_partitions':0,'source_lag':None,'raw_table_expires_at':None,'version':os.environ.get('BDC_VERSION','unknown')}


def emit(event, **fields):
    print(json.dumps({'event':event,**fields}),flush=True)


class Health(BaseHTTPRequestHandler):
    def do_GET(self):
        live = time.monotonic() - STATE['last_loop'] < 300
        if self.path == '/health/live':
            status = 200 if live else 503
        elif self.path in ('/health/ready','/status'):
            status = 200 if STATE['ready'] and live else 503
        else:
            self.send_error(404)
            return
        body=json.dumps(STATE).encode()
        self.send_response(status)
        self.send_header('Content-Type','application/json')
        self.send_header('Content-Length',str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self,*args):
        pass


async def run():
    logging.getLogger('aiokafka').setLevel(logging.CRITICAL)
    interval = int(os.environ.get('FLUSH_INTERVAL_SECONDS','300'))
    max_records = int(os.environ.get('BATCH_MAX_RECORDS','1000'))
    max_bytes = int(os.environ.get('BATCH_MAX_BYTES','1048576'))
    max_spool = int(os.environ.get('SPOOL_MAX_BYTES','33554432'))
    daily_loads = int(os.environ.get('MAX_DAILY_LOAD_ATTEMPTS','1000'))
    if not 60 <= interval <= 900 or not 1 <= max_records <= 5000 or not 65536 <= max_bytes <= 4194304:
        raise RuntimeError('invalid_batch_limits')
    if max_spool > 67108864 or max_spool < max_bytes or not 1 <= daily_loads <= 1000:
        raise RuntimeError('invalid_spool_or_quota_limits')
    journal=Journal(os.environ.get('STATE_DIR','/var/lib/warehouse'))
    sink=BigQuerySink(os.environ.get('GOOGLE_CLOUD_PROJECT','bdc-web-487000'),
                      os.environ.get('BIGQUERY_LOCATION','asia-southeast1'),
                      os.environ.get('BIGQUERY_TABLE','bdc-web-487000.bdc_raw_prod.lms_micro_interactions'))
    await asyncio.to_thread(sink.inspect)
    consumer=AIOKafkaConsumer(
        bootstrap_servers=os.environ['KAFKA_BROKERS'],group_id='bdc-warehouse-lms-v1',
        client_id='bdc-warehouse-worker', enable_auto_commit=False,auto_offset_reset='none',
        fetch_max_bytes=1048576,max_partition_fetch_bytes=1048576,
        request_timeout_ms=30000,max_poll_interval_ms=600000,
    )
    stop=asyncio.Event()
    loop=asyncio.get_running_loop()
    for sig in (signal.SIGTERM,signal.SIGINT):
        loop.add_signal_handler(sig,stop.set)
    await consumer.start()
    try:
        # Explicit single-replica assignment avoids a rebalance racing a pending
        # upload. The distinct group stores offsets, without joining app groups.
        await consumer.topics()
        partitions=consumer.partitions_for_topic(TOPIC)
        if not partitions:
            raise RuntimeError('source_topic_not_available')
        assigned=[TopicPartition(TOPIC,p) for p in sorted(partitions)]
        consumer.assign(assigned)
        starts=await consumer.beginning_offsets(assigned)
        ends=await consumer.end_offsets(assigned)
        bootstrap=json.loads(os.environ.get('BOOTSTRAP_ACKNOWLEDGED_OFFSETS','{}'))
        positions={}
        for part in assigned:
            committed=await consumer.committed(part)
            position=committed if committed is not None else bootstrap.get(str(part.partition),starts[part])
            if not starts[part] <= position <= ends[part]:
                raise RuntimeError('checkpoint_outside_retained_kafka_window')
            positions[part]=position
            consumer.seek(part,position)
        STATE['assigned_partitions']=len(assigned)
        STATE['ready']=True
        emit('worker_started',topic=TOPIC,partitions=len(assigned),flush_seconds=interval,version=STATE['version'])
        last_upload=0.0
        last_lag=0.0

        async def flush_pending():
            nonlocal last_upload
            batch=journal.pending()
            if batch is None:
                return
            # After restart or ambiguous ack, inspect this identical job before
            # any new fetch; never derive a new job ID for the same stored batch.
            if not batch['acknowledged']:
                remaining=60-(time.monotonic()-last_upload)
                if remaining>0:
                    await asyncio.sleep(remaining)
                # SQLite is owned by this thread. The SDK upload runs in a
                # worker thread so aiokafka heartbeats and health remain responsive.
                day=datetime.now(timezone.utc).date().isoformat()
                async def reserve_slot():
                    journal.reserve_load_attempt(day,daily_loads)
                def before_submit():
                    asyncio.run_coroutine_threadsafe(reserve_slot(),loop).result(timeout=10)
                rows=await asyncio.to_thread(sink.acknowledge,batch,journal.root,before_submit)
                journal.acknowledge(batch['job_id'])
                last_upload=time.monotonic()
                STATE['last_ack']=datetime.now(timezone.utc).isoformat()
                STATE['acknowledged_rows']+=rows
                STATE['successful_batches']+=1
                emit('batch_acknowledged',rows=rows,job_id=batch['job_id'])
            commit={TopicPartition(TOPIC,int(p)):OffsetAndMetadata(int(offset),'')
                    for p,offset in json.loads(batch['next_offsets']).items()}
            await consumer.commit(commit)
            for part,offset in commit.items():
                positions[part]=offset.offset
            # getmany can fetch past the byte/row boundary across partitions.
            # Rewind every partition to its durable frontier, including those
            # fetched but not represented in this acknowledged batch.
            for part,offset in positions.items():
                consumer.seek(part,offset)
            journal.finish(batch['job_id'])
            STATE['pending_records']=0

        while not stop.is_set():
            STATE['last_loop']=time.monotonic()
            try:
                if journal.pending():
                    await flush_pending()
                    STATE['ready']=True
                    STATE['last_error_class']=None
                    continue
                lines=[]
                size=0
                next_offsets={}
                first_read=time.monotonic()
                while not stop.is_set():
                    STATE['last_loop']=time.monotonic()
                    fetched=await consumer.getmany(timeout_ms=1000,max_records=256)
                    full=False
                    for part,messages in fetched.items():
                        for message in messages:
                            source=normalize_source(json.loads(message.value))
                            canonical=json.dumps(source,sort_keys=True,separators=(',',':'),allow_nan=False)
                            row=dict(source,event_id=f'{TOPIC}:{source["interaction_id"]}',
                                     ingested_at=datetime.now(timezone.utc).isoformat(timespec='microseconds'),
                                     kafka_topic=message.topic,kafka_partition=message.partition,kafka_offset=message.offset,
                                     payload_sha256=hashlib.sha256(canonical.encode()).hexdigest())
                            line=json.dumps(row,separators=(',',':'),allow_nan=False).encode()+b'\n'
                            if len(line)>max_bytes:
                                raise RuntimeError('source_record_exceeds_batch_bound')
                            if lines and (len(lines)>=max_records or size+len(line)>max_bytes):
                                full=True
                                break
                            lines.append(line)
                            size+=len(line)
                            next_offsets[str(part.partition)]=message.offset+1
                        if full:
                            break
                    STATE['pending_records']=len(lines)
                    now=time.monotonic()
                    if now-last_lag>=30:
                        latest=await consumer.end_offsets(assigned)
                        STATE['source_lag']=sum(max(0,latest[p]-positions[p]) for p in assigned)
                        STATE['raw_table_expires_at']=sink.table_expires.isoformat()
                        last_lag=now
                    if sink.table_expires <= datetime.now(timezone.utc):
                        raise RuntimeError('raw_table_expired_operator_recovery_required')
                    if full or (lines and now-first_read>=interval):
                        break
                if lines:
                    payload=b''.join(lines)
                    identity=sink.table+':'+sink.table_created.isoformat()+':'+hashlib.sha256(payload).hexdigest()
                    job_id='bdc_worker_'+hashlib.sha256(identity.encode()).hexdigest()
                    journal.prepare(job_id,payload,len(lines),next_offsets,max_spool)
                    await flush_pending()
                STATE['ready']=True
                STATE['last_error_class']=None
            except Exception as exc:
                STATE['ready']=False
                STATE['last_error_class']=type(exc).__name__
                emit('worker_retry',error_class=type(exc).__name__,pending_batch=bool(journal.pending()))
                # Rewind all fetched but non-durable records. A malformed record
                # blocks its partition; no quarantine acknowledgement is invented.
                if journal.pending() is None:
                    for part,position in positions.items():
                        consumer.seek(part,position)
                try:
                    await asyncio.wait_for(stop.wait(),timeout=30)
                except asyncio.TimeoutError:
                    pass
        STATE['ready']=False
        emit('worker_stopped',pending_batch=bool(journal.pending()))
    finally:
        await consumer.stop()


def main():
    STATE['last_loop']=time.monotonic()
    server=ThreadingHTTPServer(('0.0.0.0',8080),Health)
    threading.Thread(target=server.serve_forever,daemon=True).start()
    try:
        asyncio.run(run())
    except Exception as exc:
        emit('worker_failed',error_class=type(exc).__name__)
        raise SystemExit(1)
    finally:
        server.shutdown()


if __name__=='__main__':
    main()
