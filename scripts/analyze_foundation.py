#!/usr/bin/env python3
"""Aggregate source coverage, dimensional integrity and quality; never emit rows."""
import argparse
import json
import shutil
import sys

from warehouse import GoogleAPI,ROOT,WarehouseError,load_config,render_sql


def result_rows(api,job):
    result=api.results(job)
    names=[field['name'] for field in result.get('schema',{}).get('fields',[])]
    return [{key:value.get('v') for key,value in zip(names,row['f'])}
            for row in result.get('rows',[])]


def analyze(api,config):
    prefix=config['project_id']+'.bdc_'
    env=config['environment']
    core=lambda name:f'`{prefix}core_{env}.{name}`'
    raw=f'`{prefix}raw_{env}.platform_events`'
    source_job=api.query(f'''WITH retained AS (
      SELECT source_name,event_id,occurred_at,ingested_at FROM {raw}
      WHERE ingested_at>=TIMESTAMP_SUB(CURRENT_TIMESTAMP(),INTERVAL 60 DAY)
        AND ingested_at<TIMESTAMP_ADD(CURRENT_TIMESTAMP(),INTERVAL 1 DAY)
      UNION ALL
      SELECT 'lms.interaction',event_id,created_at,ingested_at
      FROM `{prefix}raw_{env}.lms_micro_interactions`
      WHERE ingested_at>=TIMESTAMP_SUB(CURRENT_TIMESTAMP(),INTERVAL 60 DAY)
        AND ingested_at<TIMESTAMP_ADD(CURRENT_TIMESTAMP(),INTERVAL 1 DAY)
      ) SELECT source_name,COUNT(*) AS raw_deliveries,
      COUNT(DISTINCT event_id) AS distinct_source_events,
      CAST(MIN(occurred_at) AS STRING) AS first_observed_event,
      CAST(MAX(occurred_at) AS STRING) AS latest_observed_event,
      CAST(MAX(ingested_at) AS STRING) AS latest_ingested_at
      FROM retained
      GROUP BY source_name ORDER BY source_name''')
    source_counts={row['source_name']:row for row in result_rows(api,source_job)}
    source_register=json.loads((ROOT/'contracts/sources.json').read_text())
    coverage=[]
    for source in source_register['sources']:
        observed=source_counts.get(source['name'])
        coverage.append({'source':source['name'],'owner':source['owner'],
                         'producer_status':source['producer_status'],'capture':source['capture'],
                         'retained_data_status':'observed' if observed else 'no_retained_rows_observed',
                         'observed':observed,'known_gaps':source['gaps']})
    quality_job=api.query(render_sql(config,ROOT/'checks/001_core_integrity.sql'))
    values={key:int(value) for key,value in result_rows(api,quality_job)[0].items()}
    failures=[key for key,value in values.items() if value]
    metrics_job=api.query(f"""SELECT
      (SELECT COUNT(*) FROM {core('fact_learning_activity')}) AS learning_events,
      (SELECT COUNT(*) FROM {core('fact_recommendation_event')}) AS recommendation_events,
      (SELECT COUNT(*) FROM {core('fact_llm_attempt')}) AS recorded_llm_attempts,
      (SELECT COUNT(*) FROM {core('fact_llm_attempt')} WHERE usage_status='unknown') AS llm_attempts_with_unknown_usage""")
    metrics={key:int(value) for key,value in result_rows(api,metrics_job)[0].items()}
    return {'source_coverage':coverage,'quality_counts':values,'observed_fact_counts':metrics,'quality_passed':not failures,
            'failed_checks':failures,'query_jobs':[source_job['jobReference']['jobId'],quality_job['jobReference']['jobId'],metrics_job['jobReference']['jobId']],
            'limitations':['Coverage describes retained observations, not complete source history.',
                           'No observed login rows are not evidence of zero logins.',
                           'Inferred dimensions do not establish identity descriptions or SCD history.']}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--gcloud-bin',default=shutil.which('gcloud'))
    args=parser.parse_args()
    try:
        config=load_config(ROOT/'config/project.json')
        report=analyze(GoogleAPI(config,args.gcloud_bin),config)
        print(json.dumps(report,indent=2))
        if not report['quality_passed']:
            raise SystemExit(1)
    except (WarehouseError,OSError,ValueError,TypeError) as exc:
        print('foundation_analysis: '+(str(exc) if isinstance(exc,WarehouseError) else type(exc).__name__),file=sys.stderr)
        raise SystemExit(1)


if __name__=='__main__':
    main()
