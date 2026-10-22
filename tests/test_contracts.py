"""Isolated source fixtures; never publish test events or contact production."""
import hashlib
import json

import pytest

from worker.contracts import envelope, timestamp
from worker.state import Journal


def learning_event():
    return {'interaction_id':1,'user_id':2,'course_id':3,'lesson_id':None,'node_id':None,
            'action_type':'lesson_view','score':None,'status':None,'created_at':'2026-01-01T17:05:00Z'}


def recommendation_event():
    return {'event_id':'unit-event','event_time':'2026-01-01T17:05:00Z',
            'event_type':'impression','user_id':2,'course_id':3,'recommendation_id':'unit-recommendation',
            'rank':1,'tracking_token':'must-not-land','metadata':{'prompt':'must-not-land'}}


def test_lms_identity_and_payload_hash_match_existing_landing_contract():
    source=learning_event()
    source['created_at']='2026-01-01T17:05:00.000000+00:00'
    row=envelope('lms.interaction',source,topic='lms.analytics.interactions',partition=0,offset=1)
    assert row['event_id']=='lms.analytics.interactions:1'
    expected=hashlib.sha256(json.dumps(source,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    assert row['payload_sha256']==expected
    assert row['occurred_at']=='2026-01-01T17:05:00.000000+00:00'


def test_recommendation_secret_and_content_are_excluded_before_landing():
    row=envelope('recommendation.event',recommendation_event(),
                 topic='recommender.interactions.v1',partition=0,offset=1)
    assert 'must-not-land' not in json.dumps(row)
    assert 'tracking_token' not in row['payload']
    assert 'metadata' not in row['payload']


def test_replay_hash_ignores_delivery_time_and_coordinates():
    one=envelope('lms.interaction',learning_event(),topic='lms.analytics.interactions',partition=0,offset=1)
    two=envelope('lms.interaction',learning_event(),capture_id='owner-capture',ingested_at='2026-01-02T00:00:00Z')
    assert one['event_id']==two['event_id']
    assert one['payload_sha256']==two['payload_sha256']


@pytest.mark.parametrize('value',['2026-01-01T00:00:00',None,'not-a-time'])
def test_ambiguous_or_invalid_source_time_is_rejected(value):
    with pytest.raises((ValueError,TypeError)):
        timestamp(value)


def test_journal_cannot_acknowledge_offsets_under_another_target(tmp_path):
    state=Journal(tmp_path/'state')
    state.bind_stream('group:table-a')
    state.prepare('job',b'{}\n',1,{'topic-a:0':1},1024)
    state.close()
    state=Journal(tmp_path/'state')
    with pytest.raises(RuntimeError,match='different_stream'):
        state.bind_stream('group:table-b')
    state.close()


def test_unbound_pending_journal_requires_explicit_legacy_recovery(tmp_path):
    state=Journal(tmp_path/'state')
    state.prepare('job',b'{}\n',1,{'0':1},1024)
    with pytest.raises(RuntimeError,match='operator_recovery'):
        state.bind_stream('new-group:platform')
    state.bind_stream('legacy-group:lms',allow_legacy=True)
    state.close()
