"""Exercise mixed-topic overfetch through the actual bounded consumer loop."""
import asyncio
import json
from datetime import datetime,timedelta,timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest
from aiokafka import TopicPartition

from worker import main
from worker.state import Journal
from test_contracts import learning_event,recommendation_event


@pytest.mark.asyncio
async def test_commit_only_acked_topic_and_rewind_overfetched_other_topic(tmp_path,monkeypatch):
    for key,value in {'WAREHOUSE_STREAM':'platform','STATE_DIR':str(tmp_path/'platform'),
                      'LEGACY_STATE_DIR':str(tmp_path/'legacy'),'KAFKA_BROKERS':'unit.invalid:9092',
                      'BATCH_MAX_RECORDS':'1'}.items():
        monkeypatch.setenv(key,value)
    stop=asyncio.Event()
    learning=TopicPartition('lms.analytics.interactions',0)
    recommendation=TopicPartition('recommender.interactions.v1',0)
    consumer=Mock(start=AsyncMock(),stop=AsyncMock(),committed=AsyncMock(return_value=None),
                  beginning_offsets=AsyncMock(return_value={learning:0,recommendation:0}),
                  end_offsets=AsyncMock(return_value={learning:1,recommendation:1}))
    def record(part,payload):
        return SimpleNamespace(topic=part.topic,partition=0,offset=0,value=json.dumps(payload).encode())
    consumer.getmany=AsyncMock(return_value={learning:[record(learning,learning_event())],
                                           recommendation:[record(recommendation,recommendation_event())]})
    async def commit(offsets):
        assert set(offsets)=={learning}
        assert offsets[learning].offset==1
        stop.set()
    consumer.commit=AsyncMock(side_effect=commit)
    sink=Mock(table='unit-project.unit.platform',table_created=datetime.now(timezone.utc),
              table_expires=datetime.now(timezone.utc)+timedelta(days=1),acknowledge=Mock(return_value=1))
    with patch.object(main,'AIOKafkaConsumer',return_value=consumer),patch.object(main,'BigQuerySink',return_value=sink):
        with patch.object(main,'source_partitions',new=AsyncMock(return_value=[0])):
            # Reproduce a newly booted CI/node clock below sixty seconds. The
            # first upload must not inherit an artificial pacing delay.
            task=asyncio.create_task(main.run(stop=stop,clock=lambda:5.0))
            await asyncio.wait_for(task,timeout=5)
    consumer.commit.assert_awaited_once()
    assert consumer.seek.call_args_list[-2].args==(learning,1)
    assert consumer.seek.call_args_list[-1].args==(recommendation,0)
    journal=Journal(tmp_path/'platform')
    assert journal.pending() is None
    journal.close()
