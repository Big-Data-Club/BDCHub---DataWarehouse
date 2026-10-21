"""Apply compatible warehouse DDL/views using the separate deployment identity."""
import json
import sys
import uuid
from pathlib import Path

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from warehouse import ROOT,load_config,render_sql


def main():
    config=load_config(ROOT/'config/project.json')
    client=bigquery.Client(project=config['project_id'],location=config['location'])
    for layer in ('raw','staging','core','mart','governance'):
        try:
            ds=client.get_dataset(f"{config['project_id']}.bdc_{layer}_{config['environment']}",timeout=30)
        except NotFound:
            continue
        if ds.location.lower()!=config['location'].lower():
            raise RuntimeError('existing_dataset_location_mismatch')
    for path in (ROOT/'ddl/001_foundation.sql',ROOT/'transforms/001_lms_views.sql'):
        job=client.query(render_sql(config,path),job_id='bdc_release_'+uuid.uuid4().hex,
                         job_config=bigquery.QueryJobConfig(maximum_bytes_billed=config['maximum_bytes_billed'],
                         labels={'application':'bdc','workload':'warehouse-migration'}))
        job.result(timeout=240)
        print(json.dumps({'migration':path.name,'job_id':job.job_id,'completed':True}),flush=True)


if __name__=='__main__':
    try:
        main()
    except Exception as exc:
        print(json.dumps({'migration_failed':True,'error_class':type(exc).__name__}),file=sys.stderr)
        raise SystemExit(1)
