"""Stream type classes for tap-databricks."""

from __future__ import annotations

from typing import Any

from hotglue_singer_sdk.helpers._singer import Metadata, MetadataMapping
from typing_extensions import override

from tap_databricks.client import databricksStream


def tap_stream_id(catalog_name: str, schema_name: str, table_name: str) -> str:
    """Generate tap stream id as appears in catalog.json."""
    return f"{catalog_name}_{schema_name}_{table_name}"


class DynamicStream(databricksStream):
    """Dynamic stream for a Unity Catalog table."""

    def __init__(
        self,
        tap: Any,
        catalog_name: str,
        schema_name: str,
        table_name: str,
        schema: dict,
        table_meta: dict,
        unsupported_columns: set[str] | None = None,
        replication_key: str | None = None,
        primary_keys: list[str] | None = None
    ) -> None:
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.table_meta = table_meta
        self.unsupported_columns = unsupported_columns or set()

        super().__init__(tap=tap, name=table_name, schema=schema)

        self.primary_keys = primary_keys
        self.replication_key = replication_key
        self._metadata = self._build_metadata()

    @override
    @property
    def tap_stream_id(self) -> str:
        return tap_stream_id(self.catalog_name, self.schema_name, self.name)

    def _build_metadata(
        self,
    ) -> MetadataMapping:
        valid_replication_keys = [self.replication_key] if self.replication_key else None

        mapping = MetadataMapping.get_standard_metadata(
            schema=self.schema,
            schema_name=self.schema_name,
            replication_method=self.replication_method,
            key_properties=self.primary_keys,
            valid_replication_keys=valid_replication_keys,

        )
        root = mapping.root
        setattr(root, "table_key_properties", self.primary_keys)
        setattr(root, "replication-method", self.replication_method)
        if self.replication_key:
            setattr(root, "replication-key", self.replication_key)

        setattr(root, "database-name", self.catalog_name)
        setattr(root, "is-view", self.table_meta.get("table_type") == "VIEW")
        row_count = self.table_meta.get("properties", {}).get(
            "spark.sql.statistics.numRows"
        )
        if row_count is not None:
            setattr(root, "row-count", int(row_count))
        for column_name in self.unsupported_columns:
            mapping[("properties", column_name)] = Metadata(
                inclusion=Metadata.InclusionType.UNSUPPORTED
            )
        mapping.root.selected = True
        return mapping
