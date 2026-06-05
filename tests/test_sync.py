"""Tests for SQLStream sync via DatabricksConnector."""

from __future__ import annotations

from unittest.mock import MagicMock

import sqlalchemy
from hotglue_singer_sdk.helpers._singer import CatalogEntry
from hotglue_singer_sdk.streams.core import REPLICATION_INCREMENTAL

from tap_databricks.client import DatabricksConnector
from tap_databricks.streams import DynamicStream, tap_stream_id
from tap_databricks.tap import Tapdatabricks, _uc_table_schema, build_catalog_entry_from_uc

SAMPLE_COLUMNS = [
    {"name": "id", "type_name": "INT", "nullable": True},
    {"name": "name", "type_name": "STRING", "nullable": True},
    {"name": "created_at", "type_name": "DATE", "nullable": True},
]

SAMPLE_TABLE = {
    "name": "dummy_table",
    "table_type": "MANAGED",
    "columns": SAMPLE_COLUMNS,
}


def _make_stream(
    *,
    replication_key: str | None = "created_at",
    config: dict | None = None,
) -> DynamicStream:
    schema_dict, unsupported = _uc_table_schema(SAMPLE_COLUMNS)
    entry = build_catalog_entry_from_uc(
        uc_catalog_name="workspace",
        uc_schema_name="default",
        uc_table_name="dummy_table",
        schema_dict=schema_dict,
        table_meta=SAMPLE_TABLE,
        unsupported_columns=unsupported,
        replication_key=replication_key,
        primary_keys=["id"],
    )
    tap_config = config or {
        "api_url": "https://dbc-example.cloud.databricks.com",
        "client_id": "client-id",
        "client_secret": "client-secret",
        "warehouse": "warehouse_id",
        "start_date": "2026-01-01T00:00:00Z",
    }
    tap = Tapdatabricks(config=tap_config)
    connector = DatabricksConnector(dict(tap.config))
    connector.register_table("workspace.default.dummy_table", schema_dict)
    return DynamicStream(tap=tap, catalog_entry=entry, connector=connector)


def test_get_sqlalchemy_url():
    connector = DatabricksConnector(
        {
            "api_url": "https://dbc-example.cloud.databricks.com",
            "warehouse": "abc123",
        }
    )
    url = connector.get_sqlalchemy_url(connector.config)
    assert url == (
        "databricks://token:dummy@dbc-example.cloud.databricks.com"
        "?http_path=/sql/1.0/warehouses/abc123"
    )


def test_catalog_entry_round_trip():
    schema_dict, unsupported = _uc_table_schema(SAMPLE_COLUMNS)
    entry_dict = build_catalog_entry_from_uc(
        uc_catalog_name="workspace",
        uc_schema_name="default",
        uc_table_name="dummy_table",
        schema_dict=schema_dict,
        table_meta=SAMPLE_TABLE,
        unsupported_columns=unsupported,
        replication_key="created_at",
        primary_keys=["id"],
    )
    entry = CatalogEntry.from_dict(entry_dict)
    assert entry.tap_stream_id == tap_stream_id("workspace", "default", "dummy_table")
    assert entry.replication_method == REPLICATION_INCREMENTAL
    assert entry.database == "workspace"
    assert entry.table == "dummy_table"


def test_get_records_yields_rows(monkeypatch):
    stream = _make_stream()
    mock_rows = [
        {"id": 1, "name": "Alice", "created_at": "2026-06-01"},
        {"id": 2, "name": "Bob", "created_at": "2026-06-02"},
    ]
    mock_result = MagicMock()
    mock_result.mappings.return_value = mock_rows
    mock_execute = MagicMock(return_value=mock_result)
    monkeypatch.setattr(stream.connector, "_connection", MagicMock())
    stream.connector.connection.execute = mock_execute

    records = list(stream.get_records(context=None))

    assert records == mock_rows
    mock_execute.assert_called_once()
    query = mock_execute.call_args[0][0]
    assert isinstance(query, sqlalchemy.sql.selectable.Select)


def test_get_records_incremental_where(monkeypatch):
    stream = _make_stream()
    stream._write_starting_replication_value(context=None)
    mock_result = MagicMock()
    mock_result.mappings.return_value = []
    mock_execute = MagicMock(return_value=mock_result)
    monkeypatch.setattr(stream.connector, "_connection", MagicMock())
    stream.connector.connection.execute = mock_execute

    list(stream.get_records(context=None))

    query = mock_execute.call_args[0][0]
    assert query.whereclause is not None
    assert stream.get_starting_replication_key_value(context=None) == "2026-01-01T00:00:00Z"
