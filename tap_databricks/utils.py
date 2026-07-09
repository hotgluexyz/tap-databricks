"""Unity Catalog schema and catalog entry helpers."""

from __future__ import annotations

from hotglue_singer_sdk.helpers._schema import SchemaPlus
from hotglue_singer_sdk.helpers._singer import CatalogEntry, Metadata, MetadataMapping
from hotglue_singer_sdk.streams.core import REPLICATION_FULL_TABLE, REPLICATION_INCREMENTAL

# Unity Catalog ColumnTypeName values (Databricks SDK catalog.ColumnTypeName).
_INTEGER_TYPES = frozenset({"BYTE", "SHORT", "INT"})
_NUMBER_TYPES = frozenset({"LONG", "FLOAT", "DOUBLE", "DECIMAL"})
_STRING_TYPES = frozenset({"STRING", "CHAR"})
_DATETIME_TYPES = frozenset({"TIMESTAMP", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ"})
_TIME_TYPES = frozenset({"TIME"})
_INTERVAL_TYPES = frozenset({"INTERVAL"})
_ARRAY_TYPES = frozenset({"ARRAY"})
_OBJECT_TYPES = frozenset({"MAP", "STRUCT", "VARIANT"})


def _json_schema_type(nullable: bool, *types: str) -> str | list[str]:
    if nullable:
        return ["null", *types]
    if len(types) == 1:
        return types[0]
    return list(types)


def tap_stream_id(catalog_name: str, schema_name: str, table_name: str) -> str:
    """Generate tap stream id as appears in catalog.json."""
    return f"{catalog_name}.{schema_name}.{table_name}"


def _uc_column_schema(column: dict) -> tuple[dict, bool]:
    """Map a Unity Catalog column to a JSON Schema fragment."""
    type_name = column.get("type_name", "")
    nullable = column.get("nullable", True)

    if not type_name:
        return {"description": "Unsupported data type (missing)"}, False
    elif type_name in _STRING_TYPES:
        return {"type": _json_schema_type(nullable, "string")}, True
    elif type_name in _INTEGER_TYPES:
        return {"type": _json_schema_type(nullable, "integer")}, True
    elif type_name in _NUMBER_TYPES:
        return {"type": _json_schema_type(nullable, "number")}, True
    elif type_name == "BOOLEAN":
        return {"type": _json_schema_type(nullable, "boolean")}, True
    elif type_name == "DATE":
        return {"type": _json_schema_type(nullable, "string"), "format": "date"}, True
    elif type_name in _DATETIME_TYPES:
        return {"type": _json_schema_type(nullable, "string"), "format": "date-time"}, True
    elif type_name in _TIME_TYPES:
        return {"type": _json_schema_type(nullable, "string"), "format": "time"}, True
    elif type_name in _INTERVAL_TYPES:
        return {"type": _json_schema_type(nullable, "string")}, True
    elif type_name == "BINARY":
        return {"type": _json_schema_type(nullable, "string"), "format": "binary"}, True
    elif type_name in _ARRAY_TYPES:
        return {"type": _json_schema_type(nullable, "array"), "items": {}}, True
    elif type_name in _OBJECT_TYPES:
        return {"type": _json_schema_type(nullable, "object")}, True
    else:
        return {"description": f"Unsupported data type {type_name}"}, False


def _uc_table_schema(columns: list[dict]) -> tuple[dict, frozenset[str]]:
    """Build a JSON Schema dict from Unity Catalog column metadata."""
    properties: dict = {}
    unsupported: set[str] = set()
    for column in columns:
        name = column["name"]
        prop_schema, supported = _uc_column_schema(column)
        properties[name] = prop_schema
        if not supported:
            unsupported.add(name)
    return {"type": "object", "properties": properties}, frozenset(unsupported)


def build_catalog_entry_from_uc(
    *,
    uc_catalog_name: str,
    uc_schema_name: str,
    uc_table_name: str,
    schema_dict: dict,
    table_meta: dict,
    unsupported_columns: frozenset[str] | set[str],
    replication_key: str | None,
    primary_keys: list[str],
) -> dict:
    """Build a Singer catalog_entry dict for SQLStream from Unity Catalog metadata."""
    replication_method = REPLICATION_INCREMENTAL if replication_key else REPLICATION_FULL_TABLE
    valid_replication_keys = [replication_key] if replication_key else None
    mapping = MetadataMapping.get_standard_metadata(
        schema=schema_dict,
        schema_name=uc_schema_name,
        replication_method=replication_method,
        key_properties=primary_keys or None,
        valid_replication_keys=valid_replication_keys,
    )
    root = mapping.root
    setattr(root, "table_key_properties", primary_keys)
    setattr(root, "replication-method", replication_method)
    if replication_key:
        setattr(root, "replication-key", replication_key)
    setattr(root, "database-name", uc_catalog_name)
    setattr(root, "is-view", table_meta.get("table_type") == "VIEW")
    row_count = table_meta.get("properties", {}).get("spark.sql.statistics.numRows")
    if row_count is not None:
        setattr(root, "row-count", int(row_count))
    for column_name in unsupported_columns:
        mapping[("properties", column_name)] = Metadata(
            inclusion=Metadata.InclusionType.UNSUPPORTED
        )
    mapping.root.selected = True

    entry = CatalogEntry(
        tap_stream_id=tap_stream_id(uc_catalog_name, uc_schema_name, uc_table_name),
        stream=tap_stream_id(uc_catalog_name, uc_schema_name, uc_table_name),
        # stream=uc_table_name,
        table=uc_table_name,
        database=uc_catalog_name,
        key_properties=primary_keys or None,
        schema=SchemaPlus.from_dict(schema_dict),
        is_view=table_meta.get("table_type") == "VIEW",
        replication_method=replication_method,
        replication_key=replication_key,
        metadata=mapping,
        row_count=int(row_count) if row_count is not None else None,
    )
    return entry.to_dict()
