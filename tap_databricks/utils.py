"""Unity Catalog schema and catalog entry helpers."""

from __future__ import annotations

from hotglue_singer_sdk.helpers._schema import SchemaPlus
from hotglue_singer_sdk.helpers._singer import CatalogEntry, Metadata, MetadataMapping
from hotglue_singer_sdk.streams.core import REPLICATION_FULL_TABLE, REPLICATION_INCREMENTAL

_INTEGER_TYPES = {"INT", "SHORT", "BYTE"}
_NUMBER_TYPES = {"LONG", "FLOAT", "DOUBLE", "DECIMAL"}
_DATETIME_TYPES = {"TIMESTAMP"}


def tap_stream_id(catalog_name: str, schema_name: str, table_name: str) -> str:
    """Generate tap stream id as appears in catalog.json."""
    return f"{catalog_name}.{schema_name}.{table_name}"


def _uc_column_schema(column: dict) -> tuple[dict, bool]:
    """Map a Unity Catalog column to a JSON Schema fragment."""
    type_name = column.get("type_name", "")
    nullable = column.get("nullable", True)
    types: list[str] = ["null"] if nullable else []

    if type_name == "STRING":
        types.append("string")
    elif type_name in _INTEGER_TYPES:
        types.append("integer")
    elif type_name in _NUMBER_TYPES:
        types.append("number")
    elif type_name == "BOOLEAN":
        types.append("boolean")
    elif type_name == "DATE":
        types.append("string")
    elif type_name in _DATETIME_TYPES:
        types.append("string")
    elif type_name == "BINARY":
        types.append("string")
    else:
        return {"description": f"Unsupported data type {type_name}"}, False

    schema: dict = {"type": types if len(types) > 1 else types[0]}
    if type_name == "DATE":
        schema["format"] = "date"
    elif type_name in _DATETIME_TYPES:
        schema["format"] = "date-time"
    elif type_name == "BINARY":
        schema["format"] = "binary"
    return schema, True


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
