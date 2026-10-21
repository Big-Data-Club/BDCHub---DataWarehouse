"""Isolated failure tests; these tests never contact Kafka or Google Cloud."""
from datetime import datetime,timedelta,timezone
from unittest.mock import Mock,patch

import pytest
from google.api_core.exceptions import NotFound

from worker.state import Journal
from worker.bigquery_sink import BigQuerySink


def test_unacknowledged_batch_and_offsets_survive_restart(tmp_path):
    journal=Journal(tmp_path/'state')
    journal.prepare('operational-job',b'{}\n',1,{'0':4023},1024)
    journal.close()
    recovered=Journal(tmp_path/'state')
    batch=recovered.pending()
    assert batch['payload']==b'{}\n'
    assert batch['next_offsets']=='{"0": 4023}'
    assert batch['acknowledged']==0
    with pytest.raises(RuntimeError,match='unacknowledged'):
        recovered.finish(batch['job_id'])
    recovered.close()


def test_ack_survives_restart_before_kafka_commit(tmp_path):
    journal=Journal(tmp_path/'state')
    journal.prepare('operational-job',b'{}\n',1,{'0':4023},1024)
    journal.acknowledge('operational-job')
    journal.close()
    recovered=Journal(tmp_path/'state')
    assert recovered.pending()['acknowledged']==1
    recovered.finish('operational-job')
    assert recovered.pending() is None
    recovered.close()


def test_single_writer_and_spool_limit(tmp_path):
    journal=Journal(tmp_path/'state')
    with pytest.raises(BlockingIOError):
        Journal(tmp_path/'state')
    with pytest.raises(RuntimeError,match='spool_limit'):
        journal.prepare('oversized',b'012345',1,{'0':1},4)
    assert journal.pending() is None
    journal.close()


def test_quota_is_persistent_but_does_not_remove_pending_batch(tmp_path):
    journal=Journal(tmp_path/'state')
    journal.prepare('operational-job',b'{}\n',1,{'0':4023},1024)
    journal.reserve_load_attempt('2026-10-21',1)
    journal.close()
    journal=Journal(tmp_path/'state')
    with pytest.raises(RuntimeError,match='guardrail'):
        journal.reserve_load_attempt('2026-10-21',1)
    assert journal.pending()['acknowledged']==0
    journal.close()


def sink_fixture():
    sink=BigQuerySink.__new__(BigQuerySink)
    sink.client=Mock()
    sink.table='bdc-web-487000.bdc_raw_prod.lms_micro_interactions'
    sink.location='asia-southeast1'
    sink.table_created=datetime.now(timezone.utc)-timedelta(hours=1)
    sink.inspect=Mock()
    return sink


def test_existing_job_recovers_without_resubmission_or_quota_charge():
    sink=sink_fixture()
    job=Mock(errors=None,output_rows=1,ended=datetime.now(timezone.utc))
    sink.client.get_job.return_value=job
    reserve=Mock()
    assert sink.acknowledge({'job_id':'operational-job','row_count':1},None,reserve)==1
    sink.client.load_table_from_file.assert_not_called()
    reserve.assert_not_called()


def test_missing_job_requires_submission_and_validates_ack():
    sink=sink_fixture()
    sink.client.get_job.side_effect=NotFound('not found')
    job=Mock(errors=None,output_rows=0)
    sink.client.load_table_from_file.return_value=job
    reserve=Mock()
    with pytest.raises(RuntimeError,match='acknowledgement_mismatch'):
        sink.acknowledge({'job_id':'operational-job','payload':b'{}\n','row_count':1},None,reserve)
    reserve.assert_called_once()
    assert sink.client.load_table_from_file.call_args.kwargs['job_id']=='operational-job'


def test_old_table_ack_is_not_used_for_recreated_table():
    sink=sink_fixture()
    job=Mock(errors=None,output_rows=1,ended=sink.table_created-timedelta(seconds=1))
    sink.client.get_job.return_value=job
    with pytest.raises(RuntimeError,match='previous_table_incarnation'):
        sink.acknowledge({'job_id':'operational-job','row_count':1},None,Mock())
    job.result.assert_not_called()
