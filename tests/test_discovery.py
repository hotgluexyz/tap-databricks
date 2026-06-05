"""Tests for Unity Catalog dynamic discovery."""

from __future__ import annotations

from hotglue_singer_sdk.streams.sql import SQLStream

from tap_databricks.streams import DynamicStream, tap_stream_id
from tap_databricks.tap import (
    Tapdatabricks,
    _uc_column_schema,
    _uc_table_schema,
    build_catalog_entry_from_uc,
)

SAMPLE_COLUMNS: list[dict] = [
    {
        "name": "review",
        "type_name": "STRING",
        "nullable": True,
    },
    {
        "name": "franchiseID",
        "type_name": "LONG",
        "nullable": True,
    },
    {
        "name": "review_date",
        "type_name": "TIMESTAMP",
        "nullable": True,
    },
    {
        "name": "unknown_col",
        "type_name": "GEOGRAPHY",
        "nullable": True,
    },
]

SAMPLE_TABLE = {
    "name": "media_customer_reviews",
    "catalog_name": "samples",
    "schema_name": "bakehouse",
    "table_type": "MANAGED",
    "columns": SAMPLE_COLUMNS,
    "properties": {"spark.sql.statistics.numRows": "204"},
}


def test_tap_stream_id():
    assert tap_stream_id("samples", "bakehouse", "media_customer_reviews") == (
        "samples.bakehouse.media_customer_reviews"
    )


def test_uc_column_schema_string():
    schema, supported = _uc_column_schema({"type_name": "STRING", "nullable": True})
    assert supported is True
    assert schema == {"type": ["null", "string"]}


def test_uc_column_schema_timestamp():
    schema, supported = _uc_column_schema({"type_name": "TIMESTAMP", "nullable": False})
    assert supported is True
    assert schema == {"type": "string", "format": "date-time"}


def test_uc_column_schema_unsupported():
    schema, supported = _uc_column_schema({"type_name": "GEOGRAPHY", "nullable": True})
    assert supported is False
    assert "Unsupported" in schema["description"]


def test_uc_table_schema():
    schema, unsupported = _uc_table_schema(SAMPLE_COLUMNS)
    assert set(schema["properties"]) == {
        "review",
        "franchiseID",
        "review_date",
        "unknown_col",
    }
    assert unsupported == frozenset({"unknown_col"})


def test_build_catalog_entry_from_uc():
    schema_dict, unsupported = _uc_table_schema(SAMPLE_COLUMNS)
    entry = build_catalog_entry_from_uc(
        uc_catalog_name="samples",
        uc_schema_name="bakehouse",
        uc_table_name="media_customer_reviews",
        schema_dict=schema_dict,
        table_meta=SAMPLE_TABLE,
        unsupported_columns=unsupported,
        replication_key="review_date",
        primary_keys=["review"],
    )
    assert entry["tap_stream_id"] == "samples.bakehouse.media_customer_reviews"
    assert entry["stream"] == "samples.bakehouse.media_customer_reviews"
    assert entry["replication_key"] == "review_date"
    assert entry["replication_method"] == "INCREMENTAL"
    assert entry["key_properties"] == ["review"]


def test_discover_streams(monkeypatch):
    config = {
        "api_url": "https://dbc-example.cloud.databricks.com",
        "client_id": "client-id",
        "client_secret": "client-secret",
        "oauth_scope": "all-apis",
        "start_date": "2026-01-01T00:00:00Z",
        "warehouse": "warehouse_id",
    }
    tap = Tapdatabricks(config=config)

    def mock_uc_get(path: str, params: dict | None = None) -> dict:
        if path == "/api/2.1/unity-catalog/catalogs":
            return {"catalogs": [{"name": "workspace"}, {"name": "samples"}]}
        if path == "/api/2.1/unity-catalog/schemas":
            if params == {"catalog_name": "workspace"}:
                return {"schemas": [{"name": "default"}]}
            if params == {"catalog_name": "samples"}:
                return {"schemas": [{"name": "bakehouse"}]}
        if path == "/api/2.1/unity-catalog/tables":
            if params == {"catalog_name": "workspace", "schema_name": "default"}:
                return {
                    "tables": [
                        {
                            "name": "dummy_table",
                            "table_type": "MANAGED",
                            "columns": [
                                {"name": "id", "type_name": "INT", "nullable": True},
                            ],
                        }
                    ]
                }
            if params == {"catalog_name": "samples", "schema_name": "bakehouse"}:
                return {"tables": [SAMPLE_TABLE]}
        if path.startswith("/api/2.1/unity-catalog/tables/"):
            if "workspace.default.dummy_table" in path:
                return {
                    "name": "dummy_table",
                    "table_type": "MANAGED",
                    "columns": [
                        {"name": "id", "type_name": "INT", "nullable": True},
                    ],
                }
            if "samples.bakehouse.media_customer_reviews" in path:
                return SAMPLE_TABLE
        raise AssertionError(f"Unexpected UC GET: {path} {params}")

    monkeypatch.setattr(tap, "_uc_get", mock_uc_get)
    streams = tap.discover_streams()

    assert len(streams) == 2
    assert all(isinstance(s, SQLStream) for s in streams)
    assert all(isinstance(s, DynamicStream) for s in streams)
    by_id = {s.tap_stream_id: s for s in streams}
    assert set(by_id) == {
        "workspace.default.dummy_table",
        "samples.bakehouse.media_customer_reviews",
    }
    samples_stream = by_id["samples.bakehouse.media_customer_reviews"]
    assert isinstance(samples_stream, DynamicStream)
    assert samples_stream.name == "samples.bakehouse.media_customer_reviews"
    assert samples_stream.schema["properties"]["review"]["type"] == ["null", "string"]
    assert samples_stream.metadata.root.schema_name == "bakehouse"
    assert getattr(samples_stream.metadata.root, "database-name") == "samples"
    assert getattr(samples_stream.metadata.root, "row-count") == 204
    assert samples_stream.catalog_entry["database_name"] == "samples"
    assert samples_stream.catalog_entry["table_name"] == "media_customer_reviews"
