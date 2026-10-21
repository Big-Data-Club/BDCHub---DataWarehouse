"""ADC-authenticated batch jobs with stable identities and explicit expiry checks."""
from datetime import datetime, timezone
from io import BytesIO

from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery


class BigQuerySink:
    def __init__(self, project, location, table):
        self.client = bigquery.Client(project=project, location=location)
        self.location, self.table = location, table
        self.table_created = None
        self.table_expires = None

    def inspect(self):
        table = self.client.get_table(self.table, timeout=30)
        if table.location.lower() != self.location.lower():
            raise RuntimeError('raw_table_location_mismatch')
        if not table.require_partition_filter or table.time_partitioning.field != 'ingested_at':
            raise RuntimeError('raw_table_partition_contract_mismatch')
        if not table.expires or table.expires <= datetime.now(timezone.utc):
            raise RuntimeError('raw_table_missing_or_expired')
        self.table_created, self.table_expires = table.created, table.expires
        return table

    def acknowledge(self, batch, root, before_submit):
        self.inspect()
        try:
            job = self.client.get_job(batch['job_id'], location=self.location, timeout=30)
        except NotFound:
            job = None
        if job is not None and job.ended and job.ended < self.table_created:
            # Never commit offsets based on a job for a previous table incarnation.
            # Restoring an expired table is an explicit operator replay operation.
            raise RuntimeError('batch_acknowledges_previous_table_incarnation')
        if job is None:
            before_submit()
            config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
                max_bad_records=0,
                ignore_unknown_values=False,
                labels={'application':'bdc','source':'lms','workload':'warehouse-worker'},
            )
            try:
                with BytesIO(batch['payload']) as source:
                    job = self.client.load_table_from_file(
                        source, self.table, job_id=batch['job_id'],
                        location=self.location, job_config=config, timeout=60,
                    )
            except Conflict:
                job = self.client.get_job(batch['job_id'], location=self.location, timeout=30)
        job.result(timeout=180)
        if job.errors or job.output_rows != batch['row_count']:
            raise RuntimeError('load_acknowledgement_mismatch')
        return int(job.output_rows)
