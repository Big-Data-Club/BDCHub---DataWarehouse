"""Aggregate-only integrity gate using the migration identity's existing access."""
import json
import uuid

from google.cloud import bigquery


def run_checks(client,config,root,render):
    job_id='bdc_quality_'+uuid.uuid4().hex
    print(json.dumps({'quality_job_id':job_id,'submitted':True}),flush=True)
    job=client.query(render(config,root/'checks/001_core_integrity.sql'),job_id=job_id,
        job_config=bigquery.QueryJobConfig(maximum_bytes_billed=config['maximum_bytes_billed'],
                  labels={'application':'bdc','workload':'warehouse-quality'}))
    rows=list(job.result(timeout=240))
    if len(rows)!=1:
        raise RuntimeError('quality_check_result_shape_mismatch')
    counts={key:int(value) for key,value in dict(rows[0]).items()}
    passed=all(value==0 for value in counts.values())
    print(json.dumps({'quality_job_id':job_id,'error_counts':counts,'quality_passed':passed}),flush=True)
    if not passed:
        raise RuntimeError('warehouse_model_integrity_failed')
    return counts
