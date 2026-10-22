#!/usr/bin/env python3
"""Inspect and provision the real BDC BigQuery foundation through Google CLI auth.

No credentials or source records are written by this command. Google CLI manages
and refreshes its own stored authentication; access tokens are never printed.
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
import string
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
LAYERS = ('raw', 'staging', 'core', 'mart', 'governance')
MODELS = (
    ('raw', 'lms_micro_interactions', 'TABLE'),
    ('staging', 'lms_micro_interactions', 'VIEW'),
    ('core', 'fact_content_interaction', 'VIEW'),
    ('mart', 'mart_course_daily_activity', 'VIEW'),
    ('governance', 'lms_identity_conflicts', 'VIEW'),
    ('raw','platform_events','TABLE'),
    ('staging','platform_events','VIEW'),
    ('staging','learning_events','VIEW'),
    ('governance','platform_identity_conflicts','VIEW'),
    ('governance','learning_identity_conflicts','VIEW'),
    ('core','fact_learning_activity','VIEW'),
    ('core','fact_recommendation_event','VIEW'),
    ('core','fact_llm_attempt','VIEW'),
    ('core','dim_user','VIEW'),
    ('core','dim_course','VIEW'),
    ('core','dim_feature','VIEW'),
    ('core','dim_date','VIEW'),
    ('core','dim_time','VIEW'),
    ('core','dim_ai_model','VIEW'),
)


class WarehouseError(RuntimeError):
    pass


def load_config(path: Path) -> dict:
    config = json.loads(path.read_text())
    patterns = {
        'project_id': r'[a-z][a-z0-9-]{4,28}[a-z0-9]',
        'location': r'[a-z]+-[a-z]+[0-9]',
        'environment': r'[a-z][a-z0-9_]{0,20}',
    }
    for field, pattern in patterns.items():
        if not isinstance(config.get(field), str) or not re.fullmatch(pattern, config[field]):
            raise WarehouseError(f'Invalid {field}; refusing to render SQL identifiers.')
    if config.get('mode') != 'sandbox':
        raise WarehouseError('This foundation implements the selected billing-disabled mode only.')
    limit = config.get('maximum_bytes_billed')
    if type(limit) is not int or not 0 < limit <= 1073741824:
        raise WarehouseError('maximum_bytes_billed must be a positive integer at most 1 GiB.')
    return config


def render_sql(config: dict, path: Path) -> str:
    return string.Template(path.read_text()).substitute(config)


class GoogleAPI:
    def __init__(self, config: dict, gcloud: str):
        self.config = config
        self.gcloud = gcloud
        self.token = None

    def _refresh_token(self):
        result = subprocess.run(
            [self.gcloud, 'auth', 'print-access-token', '--quiet',
             f"--project={self.config['project_id']}"],
            capture_output=True, text=True, timeout=60,
        )
        if result.returncode or not result.stdout.strip():
            raise WarehouseError('Google login could not refresh. Run gcloud auth login on this machine.')
        self.token = result.stdout.strip()

    def request(self, url: str, body=None, allow_missing=False, method=None, extra_headers=None):
        for attempt in range(2):
            if self.token is None:
                self._refresh_token()
            data = None if body is None else json.dumps(body).encode()
            headers = {
                'Authorization': f'Bearer {self.token}',
                'Content-Type': 'application/json',
            }
            headers.update(extra_headers or {})
            request = Request(url, data=data, headers=headers, method=method)
            try:
                with urlopen(request, timeout=30) as response:
                    return json.load(response)
            except HTTPError as exc:
                if exc.code == 401 and attempt == 0:
                    self.token = None
                    continue
                if exc.code == 404 and allow_missing:
                    return None
                # Only status and API reason are exposed; never echo raw API bodies,
                # submitted SQL, access tokens, source records, or provider errors.
                try:
                    details = json.loads(exc.read())
                    reason = details.get('error', {}).get('errors', [{}])[0].get('reason', 'requestFailed')
                except (ValueError, IndexError):
                    reason = 'requestFailed'
                raise WarehouseError(f'Google API HTTP {exc.code}: {reason}') from None
            except (URLError, TimeoutError):
                raise WarehouseError('Google API connection timed out or is unavailable; no acknowledgement assumed.') from None
        raise WarehouseError('Google authentication could not be refreshed.')

    @property
    def bq(self):
        return f"https://bigquery.googleapis.com/bigquery/v2/projects/{self.config['project_id']}"

    def inspect(self):
        project = self.config['project_id']
        state = self.request(f'https://cloudresourcemanager.googleapis.com/v1/projects/{project}')
        if state.get('lifecycleState') != 'ACTIVE':
            raise WarehouseError('The configured project is not ACTIVE.')
        billing = self.request(f'https://cloudbilling.googleapis.com/v1/projects/{project}/billingInfo')
        if billing.get('billingEnabled'):
            raise WarehouseError('Billing is enabled, but the chosen config requires billing disabled. No billing setting was changed.')
        report = {'project_id': project, 'checked_at': datetime.now(timezone.utc).isoformat(),
                  'authenticated': True, 'billing_enabled': False,
                  'location': self.config['location'], 'datasets': {}}
        # Dataset API access independently proves BigQuery connectivity, even when
        # all target datasets are absent. No application rows are queried here.
        self.request(f'{self.bq}/datasets?maxResults=1')
        for layer in LAYERS:
            dataset = f"bdc_{layer}_{self.config['environment']}"
            meta = self.request(f'{self.bq}/datasets/{dataset}', allow_missing=True)
            if meta is not None and meta.get('location', '').lower() != self.config['location'].lower():
                raise WarehouseError(f'{dataset} exists in a different location; refusing deployment.')
            report['datasets'][dataset] = 'exists' if meta else 'absent'
        return report

    def query(self, sql: str, dry_run=False, parameters=None):
        job_id = f'bdc_dw_{uuid.uuid4().hex}'
        body = {
            'jobReference': {'projectId': self.config['project_id'],
                             'location': self.config['location'], 'jobId': job_id},
            'configuration': {
                'dryRun': dry_run,
                'labels': {'application': 'bdc', 'workload': 'warehouse-foundation'},
                'query': {'query': sql, 'useLegacySql': False,
                          'maximumBytesBilled': str(self.config['maximum_bytes_billed'])},
            },
        }
        if parameters:
            body['configuration']['query']['parameterMode'] = 'NAMED'
            body['configuration']['query']['queryParameters'] = parameters
        # Print the operational job ID before submission so an interrupted apply
        # can be inspected without assuming failure or submitting source data twice.
        print(json.dumps({'job_id': job_id, 'dry_run': dry_run}), flush=True)
        job = self.request(f'{self.bq}/jobs', body)
        if not dry_run:
            deadline = time.monotonic() + 600
            while job.get('status', {}).get('state') != 'DONE':
                if time.monotonic() >= deadline:
                    raise WarehouseError(f'Job still pending: {job_id}; inspect it before retrying.')
                time.sleep(1)
                job = self.request(f'{self.bq}/jobs/{job_id}?' + urlencode({'location': self.config['location']}))
        error = job.get('status', {}).get('errorResult')
        if error:
            raise WarehouseError(f"BigQuery job {job_id} failed: {error.get('reason', 'unknown')}; inspect job details in Console.")
        return job

    def results(self, job):
        reference = job['jobReference']
        return self.request(f"{self.bq}/queries/{reference['jobId']}?" + urlencode({
            'location': self.config['location'], 'maxResults': 10,
        }))

    def verify(self):
        report = self.inspect()
        report['resources'] = {}
        for layer, name, expected_type in MODELS:
            dataset = f"bdc_{layer}_{self.config['environment']}"
            meta = self.request(f'{self.bq}/datasets/{dataset}/tables/{name}')
            if meta.get('type') != expected_type:
                raise WarehouseError(f'{dataset}.{name} has an unexpected resource type.')
            if not meta.get('expirationTime'):
                raise WarehouseError(f'{dataset}.{name} is missing its expected sandbox expiration.')
            if expected_type == 'TABLE':
                if not meta.get('requirePartitionFilter') or meta.get('timePartitioning', {}).get('field') != 'ingested_at':
                    raise WarehouseError('Raw table partition/filter configuration does not match the contract.')
            report['resources'][f'{dataset}.{name}'] = {
                'type': meta['type'], 'expiration_time_ms': meta['expirationTime'],
                'expires_at_utc': datetime.fromtimestamp(int(meta['expirationTime']) / 1000, timezone.utc).isoformat(),
                'metadata_row_count': meta.get('numRows') if expected_type == 'TABLE' else None,
            }
        # A dry run validates the dependency chain without scanning or returning
        # real learner records. An empty raw table is never seeded with sample rows.
        target = f"{self.config['project_id']}.bdc_mart_{self.config['environment']}.mart_course_daily_activity"
        job = self.query(f'SELECT activity_date, course_id, action_type, interaction_count FROM `{target}` LIMIT 0', dry_run=True)
        report['mart_dry_run_bytes'] = job.get('statistics', {}).get('query', {}).get('totalBytesProcessed')
        return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['check', 'render', 'apply', 'verify'])
    parser.add_argument('--config', type=Path, default=ROOT/'config/project.json')
    parser.add_argument('--gcloud-bin', default=shutil.which('gcloud'))
    args = parser.parse_args()
    try:
        config = load_config(args.config)
        files = (ROOT/'ddl/001_foundation.sql',ROOT/'ddl/003_platform_events.sql',
                 ROOT/'transforms/001_lms_views.sql',ROOT/'transforms/002_platform_core.sql')
        if args.command == 'render':
            print('\n'.join(render_sql(config, path) for path in files))
            return
        if not args.gcloud_bin:
            raise WarehouseError('Google Cloud CLI is unavailable. Set --gcloud-bin or add gcloud to PATH.')
        api = GoogleAPI(config, args.gcloud_bin)
        if args.command == 'check':
            report = api.inspect()
        elif args.command == 'apply':
            print(json.dumps(api.inspect(), indent=2), flush=True)
            for path in files:
                api.query(render_sql(config, path))
            report = api.verify()
        else:
            report = api.verify()
        print(json.dumps(report, indent=2))
    except (WarehouseError, OSError, ValueError, subprocess.TimeoutExpired) as exc:
        print(f'warehouse: {exc}', file=sys.stderr)
        raise SystemExit(1)


if __name__ == '__main__':
    main()
