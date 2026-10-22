"""Bounded, authenticated owner snapshots with durable acknowledged-ID dedupe.

No source database connection, content logging, generated records or model calls.
The AI owner supplies a repeatable-read snapshot and a verified stream trailer.
"""
import hashlib
import json
import os
import sys
from datetime import datetime,timezone,timedelta
from urllib.request import Request,urlopen

from worker.bigquery_sink import BigQuerySink
from worker.contracts import envelope
from worker.state import Journal


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()+b'\n'


def decode_snapshot(lines,max_bytes=16777216):
    header=trailer=None
    rows=[]
    digest=hashlib.sha256()
    size=0
    captured=datetime.now(timezone.utc).isoformat(timespec='microseconds')
    for line in lines:
        size+=len(line)
        if size>max_bytes or len(line)>65536:
            raise RuntimeError('owner_snapshot_exceeds_byte_bound')
        item=json.loads(line)
        kind=item.get('kind')
        if header is None:
            if kind!='header' or item.get('source')!='ai.llm_attempt' or item.get('schema_version')!=1:
                raise RuntimeError('owner_snapshot_contract_mismatch')
            if item.get('complete') is not True or type(item.get('record_count')) is not int or not 0<=item['record_count']<=10000:
                raise RuntimeError('owner_snapshot_incomplete_reduce_window')
            header=item
        elif trailer is not None:
            raise RuntimeError('owner_snapshot_trailing_data')
        elif kind=='row':
            if len(rows)>=10000:
                raise RuntimeError('owner_snapshot_exceeds_row_bound')
            digest.update(canonical(item['payload']))
            rows.append(envelope('ai.llm_attempt',item['payload'],capture_id=header['export_id'],ingested_at=captured))
        elif kind=='trailer':
            trailer=item
        else:
            raise RuntimeError('owner_snapshot_invalid_record')
    if (header is None or trailer is None or trailer.get('complete') is not True
            or trailer.get('export_id')!=header['export_id']
            or header['record_count']!=len(rows) or trailer.get('record_count')!=len(rows)
            or trailer.get('payload_sha256')!=digest.hexdigest()):
        raise RuntimeError('owner_snapshot_count_or_hash_mismatch')
    return header,rows


class SnapshotLoader:
    def __init__(self,journal,sink,max_seen=100000):
        self.journal,self.sink=journal,sink
        if not 1<=max_seen<=100000:
            raise ValueError('invalid_seen_cache_bound')
        self.max_seen=max_seen
        self.sink.inspect()
        journal.bind_stream('owner-snapshot:ai.llm_attempt:'+sink.table+':'+sink.table_created.isoformat())
        journal.db.execute('''CREATE TABLE IF NOT EXISTS seen_events (
          incarnation TEXT NOT NULL,event_id TEXT NOT NULL,payload_sha256 TEXT NOT NULL,
          ingested_date TEXT NOT NULL,PRIMARY KEY(incarnation,event_id,payload_sha256))''')
        journal.db.commit()
        self.incarnation=sink.table_created.isoformat()

    def recover(self):
        batch=self.journal.pending()
        if batch is None:
            return 0
        if not batch['acknowledged']:
            def reserve():
                self.journal.reserve_load_attempt(datetime.now(timezone.utc).date().isoformat(),1000)
            self.sink.acknowledge(batch,self.journal.root,reserve)
            self.journal.acknowledge(batch['job_id'])
        # This transaction follows durable target acknowledgement. A crash before
        # it recovers the same job; a crash after it still recovers pending finish.
        with self.journal.db:
            for line in batch['payload'].splitlines():
                row=json.loads(line)
                self.journal.db.execute('INSERT OR IGNORE INTO seen_events VALUES (?,?,?,?)',
                    (self.incarnation,row['event_id'],row['payload_sha256'],row['ingested_at'][:10]))
        self.journal.finish(batch['job_id'])
        return batch['row_count']

    def load(self,rows):
        self.recover()
        cutoff=(datetime.now(timezone.utc)-timedelta(days=60)).date().isoformat()
        with self.journal.db:
            self.journal.db.execute('DELETE FROM seen_events WHERE incarnation != ? OR ingested_date <= ?',
                                    (self.incarnation,cutoff))
        fresh=[]
        batch_keys=set()
        for row in rows:
            key=(self.incarnation,row['event_id'],row['payload_sha256'])
            if key in batch_keys:
                raise RuntimeError('duplicate_identity_in_owner_snapshot')
            batch_keys.add(key)
            found=self.journal.db.execute('SELECT 1 FROM seen_events WHERE incarnation=? AND event_id=? AND payload_sha256=?',key).fetchone()
            if found is None:
                fresh.append(row)
        if not fresh:
            return 0
        known=self.journal.db.execute('SELECT COUNT(*) FROM seen_events').fetchone()[0]
        if known+len(fresh)>self.max_seen:
            raise RuntimeError('owner_snapshot_seen_cache_limit')
        payload=b''.join(canonical(row) for row in fresh)
        identity=self.sink.table+':'+self.incarnation+':'+hashlib.sha256(payload).hexdigest()
        job_id='bdc_snapshot_'+hashlib.sha256(identity.encode()).hexdigest()
        self.journal.prepare(job_id,payload,len(fresh),{},16777216)
        return self.recover()


def main():
    secret=os.environ.get('AI_SERVICE_SECRET')
    if not secret:
        raise RuntimeError('owner_export_secret_missing')
    days=int(os.environ.get('OWNER_SNAPSHOT_DAYS','7'))
    if not 1<=days<=60:
        raise RuntimeError('invalid_owner_snapshot_window')
    journal=Journal(os.environ.get('STATE_DIR','/var/lib/warehouse/snapshot'))
    try:
        sink=BigQuerySink(os.environ['GOOGLE_CLOUD_PROJECT'],os.environ['BIGQUERY_LOCATION'],os.environ['BIGQUERY_TABLE'])
        loader=SnapshotLoader(journal,sink)
        recovered=loader.recover()
        url=os.environ.get('AI_SERVICE_URL','http://ai-service:8000').rstrip('/')+'/ai/internal/warehouse/llm-attempts?days='+str(days)
        request=Request(url,headers={'X-AI-Secret':secret,'Accept':'application/x-ndjson'})
        with urlopen(request,timeout=60) as response:
            # Limit each read as well as the whole export; a missing newline
            # cannot allocate an unbounded record before validation sees it.
            def bounded_lines():
                while True:
                    line=response.readline(65537)
                    if not line:
                        return
                    yield line
            header,rows=decode_snapshot(bounded_lines())
        loaded=loader.load(rows)
        print(json.dumps({'source':'ai.llm_attempt','source_snapshot_rows':header['record_count'],
                          'acknowledged_new_rows':loaded,'unchanged_rows':len(rows)-loaded,
                          'recovered_rows':recovered,'window_days':days,'complete':True}),flush=True)
    finally:
        journal.close()


if __name__=='__main__':
    try:
        main()
    except Exception as exc:
        print(json.dumps({'snapshot_failed':True,'error_class':type(exc).__name__}),file=sys.stderr)
        raise SystemExit(1)
