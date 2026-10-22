"""Migration permission boundaries; no network calls or production records."""
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from google.api_core.exceptions import BadRequest, NotFound

from warehouse import ROOT, load_config
from worker.migrate import apply_models


def migration_client():
    client = Mock()
    client.get_dataset.return_value = SimpleNamespace(location='asia-southeast1')
    client.query.return_value.job_id = 'migration-job'
    client.query.return_value.result.return_value = [{'unit_error_count':0}]
    return client


def test_release_only_modifies_models_in_existing_datasets(capsys):
    client = migration_client()
    apply_models(client, load_config(ROOT / 'config/project.json'))
    assert client.get_dataset.call_count == 5
    queries = [call.args[0] for call in client.query.call_args_list]
    assert len(queries) == 5
    assert all('CREATE SCHEMA' not in sql.upper() for sql in queries)
    assert 'CREATE TABLE IF NOT EXISTS' in queries[0]
    assert 'CREATE OR REPLACE VIEW' in queries[-2]
    assert client.query.return_value.result.call_count == 5
    assert '"completed": true' in capsys.readouterr().out


def test_missing_dataset_requires_operator_bootstrap():
    client = migration_client()
    client.get_dataset.side_effect = NotFound('missing')
    with pytest.raises(RuntimeError, match='runtime_dataset_missing'):
        apply_models(client, load_config(ROOT / 'config/project.json'))
    client.query.assert_not_called()


def test_location_mismatch_prevents_any_model_changes():
    client = migration_client()
    client.get_dataset.return_value.location = 'US'
    with pytest.raises(RuntimeError, match='existing_dataset_location_mismatch'):
        apply_models(client, load_config(ROOT / 'config/project.json'))
    client.query.assert_not_called()


def test_failed_job_is_identifiable_and_stops_remaining_migrations(capsys):
    client = migration_client()
    client.query.return_value.result.side_effect = BadRequest('private provider detail')
    with pytest.raises(BadRequest):
        apply_models(client, load_config(ROOT / 'config/project.json'))
    assert client.query.call_count == 1
    output = capsys.readouterr().out
    assert '"job_id": "bdc_release_' in output
    assert 'private provider detail' not in output
    assert '"completed": true' not in output


def test_integrity_failure_blocks_release_after_ddl(capsys):
    client=migration_client()
    client.query.return_value.result.side_effect=[None,None,None,None,[{'orphan_user':1}]]
    with pytest.raises(RuntimeError,match='model_integrity_failed'):
        apply_models(client,load_config(ROOT/'config/project.json'))
    assert '"quality_passed": false' in capsys.readouterr().out
