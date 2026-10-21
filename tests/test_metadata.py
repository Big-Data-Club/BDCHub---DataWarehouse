"""Partition discovery must not depend on an unsubscribed consumer cache."""
from unittest.mock import AsyncMock, patch

import pytest

from worker.main import TOPIC, source_partitions


@pytest.mark.asyncio
async def test_explicit_assignment_discovers_all_broker_partitions():
    admin=AsyncMock()
    admin.describe_topics.return_value=[{'topic':TOPIC,'error_code':0,'partitions':[
        {'partition':2,'error_code':0}, {'partition':0,'error_code':0},
    ]}]
    with patch('worker.main.AIOKafkaAdminClient',return_value=admin):
        assert await source_partitions('broker.invalid:9092') == [0,2]
    admin.describe_topics.assert_awaited_once_with([TOPIC])
    admin.close.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize('metadata',[
    [],
    [{'topic':TOPIC,'error_code':3,'partitions':[]}],
    [{'topic':TOPIC,'error_code':0,'partitions':[]}],
    [{'topic':TOPIC,'error_code':0,'partitions':[{'partition':0,'error_code':5}]}],
])
async def test_unavailable_metadata_fails_closed_and_closes_connection(metadata):
    admin=AsyncMock()
    admin.describe_topics.return_value=metadata
    with patch('worker.main.AIOKafkaAdminClient',return_value=admin):
        with pytest.raises(RuntimeError,match='source_'):
            await source_partitions('broker.invalid:9092')
    admin.close.assert_awaited_once()
