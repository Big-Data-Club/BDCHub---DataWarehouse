"""Owner snapshot replay/recovery without HTTP or Google Cloud calls."""
import hashlib
import json
from datetime import datetime,timedelta,timezone
from unittest.mock import Mock

import pytest

from worker.contracts import envelope
from worker.snapshot import SnapshotLoader,canonical,decode_snapshot
from worker.state import Journal


def usage_payload():
    return {'usage_id':1,'created_at':'2026-01-01T00:00:00Z','task_code':'unit-task',
            'provider_code':'unit-provider','model_name':'unit-model','request_id':None,
            'prompt_tokens':5,'completion_tokens':2,'latency_ms':10,'success':True,
            'fallback_used':False,'attempt_no':1,'usage_status':'reported'}


def snapshot_lines():
    p=usage_payload()
    return [canonical({'kind':'header','schema_version':1,'source':'ai.llm_attempt',
                       'export_id':'unit-snapshot','record_count':1,'complete':True}),
            canonical({'kind':'row','payload':p}),
            canonical({'kind':'trailer','export_id':'unit-snapshot','record_count':1,
                       'complete':True,'payload_sha256':hashlib.sha256(canonical(p)).hexdigest()})]


def sink():
    return Mock(table='unit-project.unit.platform',table_created=datetime.now(timezone.utc),
                acknowledge=Mock(side_effect=lambda batch,*args:batch['row_count']))


def test_seen_cache_is_bounded_before_accepting_another_batch(tmp_path):
    journal=Journal(tmp_path/'state')
    target=sink()
    loader=SnapshotLoader(journal,target,max_seen=1)
    _,rows=decode_snapshot(snapshot_lines())
    assert loader.load(rows)==1
    second=envelope('ai.llm_attempt',{**usage_payload(),'usage_id':2},capture_id='second')
    with pytest.raises(RuntimeError,match='seen_cache_limit'):
        loader.load([second])
    assert journal.pending() is None
    target.acknowledge.assert_called_once()
    journal.close()


def test_snapshot_requires_complete_count_and_hash():
    assert len(decode_snapshot(snapshot_lines())[1])==1
    with pytest.raises(RuntimeError,match='count_or_hash'):
        decode_snapshot(snapshot_lines()[:-1])
    lines=snapshot_lines()
    end=json.loads(lines[-1]); end['record_count']=2; lines[-1]=canonical(end)
    with pytest.raises(RuntimeError,match='count_or_hash'):
        decode_snapshot(lines)


def test_partial_oversized_export_is_not_accepted():
    with pytest.raises(RuntimeError,match='incomplete'):
        decode_snapshot([canonical({'kind':'header','schema_version':1,'source':'ai.llm_attempt',
                                   'record_count':10001,'complete':False})])


def test_repeated_owner_export_skips_confirmed_rows(tmp_path):
    journal=Journal(tmp_path/'state')
    target=sink()
    loader=SnapshotLoader(journal,target)
    _,rows=decode_snapshot(snapshot_lines())
    assert loader.load(rows)==1
    # A new delivery timestamp/capture ID does not change the source identity/hash.
    replay=envelope('ai.llm_attempt',usage_payload(),capture_id='later-export')
    assert loader.load([replay])==0
    target.acknowledge.assert_called_once()
    journal.close()


def test_failed_upload_keeps_pending_and_does_not_mark_seen(tmp_path):
    journal=Journal(tmp_path/'state')
    target=sink()
    target.acknowledge.side_effect=RuntimeError('unit_upload_failure')
    loader=SnapshotLoader(journal,target)
    _,rows=decode_snapshot(snapshot_lines())
    with pytest.raises(RuntimeError,match='unit_upload_failure'):
        loader.load(rows)
    pending_id=journal.pending()['job_id']
    assert journal.db.execute('SELECT COUNT(*) FROM seen_events').fetchone()[0]==0
    journal.close()
    journal=Journal(tmp_path/'state')
    target.acknowledge.side_effect=lambda batch,*args:batch['row_count']
    loader=SnapshotLoader(journal,target)
    assert loader.recover()==1
    assert target.acknowledge.call_args.args[0]['job_id']==pending_id
    assert journal.pending() is None
    journal.close()


def test_acknowledged_pending_batch_is_not_reused_after_target_recreation(tmp_path):
    journal=Journal(tmp_path/'state')
    target=sink()
    SnapshotLoader(journal,target)
    journal.prepare('old-job',b'{}\n',1,{},1024)
    journal.acknowledge('old-job')
    journal.close()
    target.table_created+=timedelta(hours=1)
    journal=Journal(tmp_path/'state')
    with pytest.raises(RuntimeError,match='different_stream'):
        SnapshotLoader(journal,target)
    assert journal.pending()['acknowledged']==1
    journal.close()
