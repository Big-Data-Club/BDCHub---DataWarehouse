#!/usr/bin/env python3
"""Capture a bounded real Kafka snapshot inside the Personalize runtime.

Uses its existing aiokafka dependency and broker configuration. No consumer group
is joined and no Kafka offset is committed. Raw records go only to a private file;
stdout contains counts/offsets, never learner records.
"""
import argparse
import asyncio
import hashlib
import json
import logging
import math
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

TOPIC = 'lms.analytics.interactions'
FIELDS = ('interaction_id', 'user_id', 'course_id', 'lesson_id', 'node_id',
          'action_type', 'score', 'status', 'created_at')


def normalize_source(value):
    if not isinstance(value, dict):
        raise ValueError('payload_not_object')
    result = {name: value.get(name) for name in FIELDS}
    for name in ('interaction_id', 'user_id', 'course_id', 'lesson_id', 'node_id'):
        number = result[name]
        if number is None and name in ('lesson_id', 'node_id'):
            continue
        if type(number) is not int or not 0 < number <= 9223372036854775807:
            raise ValueError('invalid_source_identifier')
    for name in ('action_type', 'status'):
        word = result[name]
        if name == 'status' and word is None:
            continue
        if not isinstance(word, str) or not re.fullmatch(r'[a-zA-Z][a-zA-Z0-9_\-]{0,63}', word):
            raise ValueError('invalid_controlled_string')
    score = result['score']
    if score is not None and (type(score) not in (int, float) or not math.isfinite(score)):
        raise ValueError('invalid_score')
    stamp = result['created_at']
    if not isinstance(stamp, str):
        raise ValueError('missing_event_time')
    parsed = datetime.fromisoformat(stamp.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('event_time_missing_timezone')
    result['created_at'] = parsed.astimezone(timezone.utc).isoformat(timespec='microseconds')
    return result


async def capture(args):
    from aiokafka import AIOKafkaConsumer
    from app.core.config import get_settings
    logging.getLogger('aiokafka').setLevel(logging.CRITICAL)
    consumer = AIOKafkaConsumer(
        TOPIC, bootstrap_servers=get_settings().kafka_brokers,
        group_id=None, enable_auto_commit=False, auto_offset_reset='earliest',
        client_id='bdc-warehouse-bounded-capture',
        fetch_max_bytes=1048576, max_partition_fetch_bytes=1048576,
        request_timeout_ms=10000,
    )
    rows, encoded_bytes = [], 0
    await consumer.start()
    try:
        assigned = consumer.assignment()
        if not assigned:
            raise RuntimeError('topic_has_no_assigned_partitions')
        starts = await consumer.beginning_offsets(assigned)
        ends = await consumer.end_offsets(assigned)
        next_offsets = dict(starts)
        for part in assigned:
            consumer.seek(part, starts[part])
        pending = {p for p in assigned if starts[p] < ends[p]}
        for part in assigned - pending:
            consumer.pause(part)
        deadline = time.monotonic() + args.timeout
        complete = True
        captured_at = datetime.now(timezone.utc).isoformat(timespec='microseconds')
        while pending:
            if time.monotonic() > deadline:
                raise RuntimeError('snapshot_read_timeout_no_batch_written')
            fetched = await consumer.getmany(timeout_ms=1000, max_records=256)
            bounded = False
            for part, messages in fetched.items():
                for message in messages:
                    if message.offset >= ends[part]:
                        break
                    source = normalize_source(json.loads(message.value))
                    canonical = json.dumps(source, sort_keys=True, separators=(',', ':'), allow_nan=False)
                    row = dict(source, event_id=f'{TOPIC}:{source["interaction_id"]}',
                               ingested_at=captured_at, kafka_topic=message.topic,
                               kafka_partition=message.partition, kafka_offset=message.offset,
                               payload_sha256=hashlib.sha256(canonical.encode()).hexdigest())
                    line = json.dumps(row, separators=(',', ':'), allow_nan=False).encode() + b'\n'
                    if len(rows) >= args.max_records or encoded_bytes + len(line) > args.max_bytes:
                        bounded = True
                        complete = False
                        break
                    rows.append(line)
                    encoded_bytes += len(line)
                    next_offsets[part] = message.offset + 1
                if bounded:
                    break
                if await consumer.position(part) >= ends[part]:
                    # Kafka offsets can have gaps; position is safe here because
                    # all returned records below the snapshot bound were processed.
                    next_offsets[part] = ends[part]
                    pending.discard(part)
                    consumer.pause(part)
            if bounded:
                break
        args.output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        fd = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'wb') as handle:
            for line in rows:
                handle.write(line)
        return {'source_topic': TOPIC, 'exported_records': len(rows),
                'exported_bytes': encoded_bytes, 'snapshot_complete': complete,
                'consumer_group': None, 'offsets_committed': False,
                'partitions': [{'partition': p.partition, 'begin': starts[p],
                                'snapshot_end': ends[p], 'next_export_offset': next_offsets[p]}
                               for p in sorted(assigned, key=lambda p: p.partition)]}
    finally:
        await consumer.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--max-records', type=int, default=1000)
    parser.add_argument('--max-bytes', type=int, default=4194304)
    parser.add_argument('--timeout', type=int, default=30)
    args = parser.parse_args()
    if not 1 <= args.max_records <= 10000 or not 1 <= args.max_bytes <= 16777216 or not 1 <= args.timeout <= 60:
        parser.error('Capture bounds exceed the supported one-shot limits.')
    try:
        print(json.dumps(asyncio.run(capture(args))))
    except Exception as exc:
        # Exception class only: decoder errors can otherwise echo raw payloads.
        print(json.dumps({'capture_failed': True, 'error_class': type(exc).__name__}), file=sys.stderr)
        raise SystemExit(1)


if __name__ == '__main__':
    main()
