#!/usr/bin/env python3
"""Batch-load a private, bounded real Kafka capture; report metadata only."""
import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import urlencode

from export_lms_kafka import FIELDS, TOPIC, normalize_source
from warehouse import GoogleAPI, ROOT, WarehouseError, load_config

EXTRA = {'event_id', 'ingested_at', 'kafka_topic', 'kafka_partition', 'kafka_offset', 'payload_sha256'}


def validate_batch(path):
    from datetime import datetime, timezone
    if not path.is_file() or path.stat().st_size > 16777216:
        raise WarehouseError('Capture file is absent or exceeds the 16 MiB bound.')
    digest = hashlib.sha256()
    coordinates = set()
    rows = 0
    with path.open('rb') as handle:
        for line in handle:
            digest.update(line)
            row = json.loads(line)
            if set(row) != set(FIELDS) | EXTRA:
                raise WarehouseError('Capture schema differs from the allowlisted Kafka contract.')
            source = normalize_source(row)
            canonical = json.dumps(source, sort_keys=True, separators=(',', ':'), allow_nan=False)
            if row['payload_sha256'] != hashlib.sha256(canonical.encode()).hexdigest():
                raise WarehouseError('Source payload hash does not match; batch was not submitted.')
            if row['event_id'] != f'{TOPIC}:{source["interaction_id"]}' or row['kafka_topic'] != TOPIC:
                raise WarehouseError('Source identity or topic does not match the LMS contract.')
            for name in ('kafka_partition', 'kafka_offset'):
                if type(row[name]) is not int or row[name] < 0:
                    raise WarehouseError('Missing/invalid original Kafka coordinate.')
            captured = datetime.fromisoformat(row['ingested_at'].replace('Z', '+00:00'))
            if captured.tzinfo is None or captured > datetime.now(timezone.utc):
                raise WarehouseError('Invalid real capture timestamp.')
            key = row['kafka_partition'], row['kafka_offset']
            if key in coordinates:
                raise WarehouseError('A Kafka coordinate is duplicated inside this batch.')
            coordinates.add(key)
            rows += 1
            if rows > 10000:
                raise WarehouseError('Capture exceeds the record bound.')
    if not rows:
        raise WarehouseError('The source capture is empty; no data was generated or submitted.')
    return rows, digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('capture', type=Path)
    parser.add_argument('--config', type=Path, default=ROOT/'config/project.json')
    parser.add_argument('--gcloud-bin', default=shutil.which('gcloud'))
    args = parser.parse_args()
    try:
        if not args.gcloud_bin:
            raise WarehouseError('Google Cloud CLI is required.')
        config = load_config(args.config)
        rows, digest = validate_batch(args.capture)
        target = f"{config['project_id']}:bdc_raw_{config['environment']}.lms_micro_interactions"
        job_id = 'bdc_lms_' + hashlib.sha256((target + ':' + digest).encode()).hexdigest()
        api = GoogleAPI(config, args.gcloud_bin)
        api.inspect()
        url = f'{api.bq}/jobs/{job_id}?' + urlencode({'location': config['location']})
        job = api.request(url, allow_missing=True)
        table_url = f"{api.bq}/datasets/bdc_raw_{config['environment']}/tables/lms_micro_interactions"
        table = api.request(table_url)
        if job is not None:
            acknowledged_at = int(job.get('statistics', {}).get('endTime', '0'))
            table_created_at = int(table['creationTime'])
            if acknowledged_at and acknowledged_at < table_created_at:
                # A successful old load does not acknowledge data in a table
                # recreated after Sandbox expiry. Reuse is scoped to the table
                # incarnation, without changing current successful batch IDs.
                job_id = 'bdc_lms_' + hashlib.sha256(
                    (target + ':' + digest + ':' + str(table_created_at)).encode()
                ).hexdigest()
                url = f'{api.bq}/jobs/{job_id}?' + urlencode({'location': config['location']})
                job = api.request(url, allow_missing=True)
        reused = job is not None
        if job is None:
            sdk_bin = Path(args.gcloud_bin).resolve().parent
            bq = sdk_bin/'bq'
            if not bq.is_file():
                raise WarehouseError('bq is missing beside gcloud.')
            environment = dict(os.environ, PATH=str(sdk_bin) + os.pathsep + os.environ.get('PATH', ''))
            # Capture stdout/stderr because load errors can quote rejected source
            # values. Never print learner records or raw load error messages.
            result = subprocess.run([
                str(bq), f"--project_id={config['project_id']}",
                f"--location={config['location']}", f'--job_id={job_id}', '--format=json',
                'load', '--source_format=NEWLINE_DELIMITED_JSON', '--max_bad_records=0',
                target, str(args.capture.resolve()),
            ], env=environment, capture_output=True, text=True, timeout=300)
            job = api.request(url, allow_missing=True)
            if job is None:
                raise WarehouseError(f'No acknowledgement for {job_id}; retain the capture and inspect this job before retrying.')
        deadline = time.monotonic() + 300
        while job.get('status', {}).get('state') != 'DONE':
            if time.monotonic() > deadline:
                raise WarehouseError(f'Load still pending: {job_id}; retain the original capture.')
            time.sleep(1)
            job = api.request(url)
        if job.get('status', {}).get('errorResult'):
            reason = job['status']['errorResult'].get('reason', 'unknown')
            raise WarehouseError(f'Load {job_id} failed: {reason}; source data was not logged.')
        actual = int(job.get('statistics', {}).get('load', {}).get('outputRows', '-1'))
        if actual != rows:
            raise WarehouseError(f'Load acknowledgement count differs from the validated batch: {job_id}.')
        print(json.dumps({'project_id': config['project_id'], 'load_job_id': job_id,
                          'validated_source_rows': rows, 'acknowledged_rows': actual,
                          'reused_existing_job': reused, 'source_topic': TOPIC,
                          'kafka_offsets_committed': False}, indent=2))
    except (WarehouseError, OSError, ValueError, subprocess.TimeoutExpired, TypeError) as exc:
        # Validation exceptions must not echo source text/values.
        message = str(exc) if isinstance(exc, WarehouseError) else type(exc).__name__
        print(f'load_lms_batch: {message}', file=sys.stderr)
        raise SystemExit(1)


if __name__ == '__main__':
    main()
