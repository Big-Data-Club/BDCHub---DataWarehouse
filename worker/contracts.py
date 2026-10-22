"""Versioned source allowlists. No raw content or credential fields are landed."""
import hashlib
import json
import re
from datetime import datetime, timezone

from scripts.export_lms_kafka import normalize_source

TOPICS = {'lms.analytics.interactions':'lms.interaction',
          'recommender.interactions.v1':'recommendation.event'}
OWNERS = {'lms.interaction':'lms-service','recommendation.event':'recommender-service',
          'ai.llm_attempt':'ai-service'}


def timestamp(value):
    if not isinstance(value,str):
        raise ValueError('timestamp_required')
    parsed=datetime.fromisoformat(value.replace('Z','+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('timezone_required')
    return parsed.astimezone(timezone.utc).isoformat(timespec='microseconds')


def identifier(value, nullable=False):
    if value is None and nullable:
        return None
    if type(value) is not int or not 0 < value <= 9223372036854775807:
        raise ValueError('positive_integer_required')
    return value


def label(value, limit=120, nullable=False):
    if value is None and nullable:
        return None
    if not isinstance(value,str) or not 0<len(value)<=limit or not re.fullmatch(r'[A-Za-z0-9_./:@+ -]+',value):
        raise ValueError('invalid_controlled_label')
    return value


def normalize(name, payload):
    if not isinstance(payload,dict):
        raise ValueError('object_required')
    if name=='lms.interaction':
        p=normalize_source(payload)
        return p, str(p['interaction_id']), p['created_at'], p['user_id'], p['course_id'], None, None
    if name=='recommendation.event':
        fields=('event_id','event_time','event_type','user_id','course_id','recommendation_id',
                'recommendation_set_id','surface','entity_type','entity_id','rank')
        p={k:payload.get(k) for k in fields}
        p['event_id']=label(p['event_id'])
        p['event_time']=timestamp(p['event_time'])
        if p['event_type'] not in {'impression','click','accept','reject','dismiss','started','completed'}:
            raise ValueError('unsupported_recommendation_event')
        p['user_id']=identifier(p['user_id'])
        p['course_id']=identifier(p['course_id'],True)
        p['recommendation_id']=label(p['recommendation_id'])
        for k in ('recommendation_set_id','surface','entity_type','entity_id'):
            p[k]=label(p[k],nullable=True)
        if p['rank'] is not None and (type(p['rank']) is not int or not 1<=p['rank']<=50):
            raise ValueError('invalid_rank')
        return p,p['event_id'],p['event_time'],p['user_id'],p['course_id'],None,None
    if name=='ai.llm_attempt':
        fields=('usage_id','created_at','task_code','provider_code','model_name','request_id',
                'prompt_tokens','completion_tokens','latency_ms','success','fallback_used',
                'attempt_no','usage_status','error_class')
        p={k:payload.get(k) for k in fields}
        identifier(p['usage_id'])
        p['created_at']=timestamp(p['created_at'])
        for k in ('task_code','provider_code','model_name','request_id'):
            p[k]=label(p[k],nullable=k!='task_code')
        for k in ('prompt_tokens','completion_tokens','latency_ms'):
            if p[k] is not None and (type(p[k]) is not int or p[k]<0):
                raise ValueError('invalid_nonnegative_measure')
        if type(p['success']) is not bool or type(p['fallback_used']) is not bool:
            raise ValueError('boolean_required')
        identifier(p['attempt_no'])
        if p['usage_status'] not in {'reported','unknown'}:
            raise ValueError('invalid_usage_status')
        if p['usage_status']=='unknown' and (p['prompt_tokens'] is not None or p['completion_tokens'] is not None):
            raise ValueError('unknown_usage_must_be_null')
        if p['error_class'] not in {None,'auth','context_length','empty_completion','rate_limited',
                                    'unexpected','invalid_response','provider_error','unclassified'}:
            raise ValueError('invalid_error_class')
        return p,str(p['usage_id']),p['created_at'],None,None,p['request_id'],None
    raise ValueError('source_not_onboarded')


def envelope(name,payload,*,ingested_at=None,topic=None,partition=None,offset=None,capture_id=None):
    p,key,event_time,user,course,request,session=normalize(name,payload)
    canonical=json.dumps(p,sort_keys=True,separators=(',',':'),allow_nan=False)
    if topic is not None:
        if TOPICS.get(topic)!=name or type(partition) is not int or partition<0 or type(offset) is not int or offset<0:
            raise ValueError('invalid_source_coordinates')
    elif not capture_id:
        raise ValueError('capture_identity_required')
    # Preserve the already-deployed LMS event identity across the new landing path.
    namespace='lms.analytics.interactions' if name=='lms.interaction' else name
    return {'event_id':namespace+':'+key,'source_name':name,'schema_version':1,
            'record_source':OWNERS[name],'source_event_id':key,'occurred_at':timestamp(event_time),
            'ingested_at':timestamp(ingested_at or datetime.now(timezone.utc).isoformat()),
            'user_id':user,'course_id':course,'request_id':request,'session_id':session,
            'capture_method':'kafka' if topic is not None else 'owner_snapshot',
            'capture_id':capture_id,'kafka_topic':topic,'kafka_partition':partition,'kafka_offset':offset,
            'payload_sha256':hashlib.sha256(canonical.encode()).hexdigest(),'payload':p}
