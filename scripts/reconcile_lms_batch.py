#!/usr/bin/env python3
"""Reconcile a real source capture with raw, deduplicated facts, and daily marts.

Only aggregate counts are printed. No source/user/course identifiers are returned.
"""
import argparse
import json
import shutil
import sys
from pathlib import Path

from load_lms_batch import validate_batch
from warehouse import GoogleAPI, ROOT, WarehouseError, load_config


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('capture', type=Path)
    parser.add_argument('--config', type=Path, default=ROOT/'config/project.json')
    parser.add_argument('--gcloud-bin', default=shutil.which('gcloud'))
    args = parser.parse_args()
    try:
        count, _ = validate_batch(args.capture)
        config = load_config(args.config)
        expected = []
        event_ids = set()
        with args.capture.open() as handle:
            for line in handle:
                row = json.loads(line)
                event_ids.add(row['event_id'])
                expected.append({name: row[name] for name in
                                 ('event_id', 'kafka_partition', 'kafka_offset', 'payload_sha256')})
        project, env = config['project_id'], config['environment']
        sql = f'''
WITH expected AS (
  SELECT
    JSON_VALUE(item, '$.event_id') AS event_id,
    CAST(JSON_VALUE(item, '$.kafka_partition') AS INT64) AS kafka_partition,
    CAST(JSON_VALUE(item, '$.kafka_offset') AS INT64) AS kafka_offset,
    JSON_VALUE(item, '$.payload_sha256') AS payload_sha256
  FROM UNNEST(JSON_QUERY_ARRAY(PARSE_JSON(@capture_coordinates))) AS item
), selected_raw AS (
  SELECT r.*, e.payload_sha256 AS expected_hash
  FROM `{project}.bdc_raw_{env}.lms_micro_interactions` r
  JOIN expected e USING (event_id, kafka_partition, kafka_offset)
  WHERE r.ingested_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 DAY)
    AND r.ingested_at < TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
), unique_expected AS (SELECT DISTINCT event_id FROM expected)
SELECT
  (SELECT COUNT(*) FROM selected_raw) AS raw_delivery_rows,
  (SELECT COUNT(*) FROM selected_raw WHERE payload_sha256 != expected_hash) AS payload_mismatches,
  (SELECT COUNT(*) FROM (SELECT DISTINCT kafka_partition, kafka_offset FROM selected_raw)) AS acknowledged_coordinates,
  (SELECT COUNT(*) FROM `{project}.bdc_core_{env}.fact_content_interaction` JOIN unique_expected USING (event_id)) AS captured_fact_events,
  (SELECT COUNT(*) FROM `{project}.bdc_governance_{env}.lms_identity_conflicts` JOIN unique_expected USING (event_id)) AS captured_identity_conflicts,
  (SELECT COUNT(*) FROM `{project}.bdc_core_{env}.fact_content_interaction`) AS total_fact_events,
  (SELECT COALESCE(SUM(interaction_count), 0) FROM `{project}.bdc_mart_{env}.mart_course_daily_activity`) AS total_mart_interactions
'''
        api = GoogleAPI(config, args.gcloud_bin)
        job = api.query(sql, parameters=[{
            'name': 'capture_coordinates', 'parameterType': {'type': 'STRING'},
            'parameterValue': {'value': json.dumps(expected, separators=(',', ':'))},
        }])
        result = api.results(job)
        values = result.get('rows', [])
        if len(values) != 1:
            raise WarehouseError('Reconciliation did not return its expected aggregate row.')
        report = {field['name']: int(value['v'])
                  for field, value in zip(result['schema']['fields'], values[0]['f'])}
        report.update(source_capture_records=count, source_distinct_events=len(event_ids))
        passed = (
            report['acknowledged_coordinates'] == count
            and report['raw_delivery_rows'] >= count
            and report['payload_mismatches'] == 0
            and report['captured_fact_events'] == len(event_ids)
            and report['captured_identity_conflicts'] == 0
            and report['total_fact_events'] == report['total_mart_interactions']
        )
        report['reconciliation_passed'] = passed
        print(json.dumps(report, indent=2))
        if not passed:
            raise SystemExit(1)
    except (WarehouseError, OSError, ValueError, TypeError) as exc:
        message = str(exc) if isinstance(exc, WarehouseError) else type(exc).__name__
        print(f'reconcile_lms_batch: {message}', file=sys.stderr)
        raise SystemExit(1)


if __name__ == '__main__':
    main()
