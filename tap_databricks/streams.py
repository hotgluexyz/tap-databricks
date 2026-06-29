"""Stream type classes for tap-databricks."""

from __future__ import annotations

from typing import Any, Iterable, Dict, Optional

import sqlalchemy
from hotglue_singer_sdk.streams.sql import SQLConnector, SQLStream
from hotglue_singer_sdk.helpers._typing import conform_record_data_types
from hotglue_singer_sdk.streams.core import REPLICATION_FULL_TABLE

from tap_databricks.client import DatabricksConnector


class DynamicStream(SQLStream):
    """Dynamic stream for a Unity Catalog table."""

    connector_class = DatabricksConnector

    def __init__(
        self,
        tap: Any,
        catalog_entry: dict,
        connector: SQLConnector | None = None,
    ) -> None:
        super().__init__(tap=tap, catalog_entry=catalog_entry, connector=connector)
        entry = self._singer_catalog_entry
        if entry.replication_key:
            self.replication_key = entry.replication_key
        if entry.replication_method:
            self.forced_replication_method = entry.replication_method

    def get_estimated_record_count(self) -> Optional[int]:
        if self._singer_catalog_entry.replication_method == REPLICATION_FULL_TABLE:
            return self.catalog_entry.get("row_count")

        self._write_starting_replication_value(None)
        table = self.connector.get_table(self.fully_qualified_name)
        query = sqlalchemy.select(sqlalchemy.func.count()).select_from(table)
        replication_key_col = table.columns[self.replication_key]
        start_val = self.get_starting_replication_key_value(None)
        query = query.where(replication_key_col >= start_val)

        return self.connector.connection.execute(query).scalar_one()

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        If the stream has a replication_key value defined, records will be sorted by the
        incremental key. If the stream also has an available starting bookmark, the
        records will be filtered for values greater than or equal to the bookmark value.

        Yields:
            One dict per record.
        """

        table = self.connector.get_table(self.fully_qualified_name)
        query = table.select()
        if self.replication_key:
            replication_key_col = table.columns[self.replication_key]
            query = query.order_by(replication_key_col)

            start_val = self.get_starting_replication_key_value(context)
            if start_val is not None:
                # Use Core comparison — proper Executable, clean bind for start_val only
                query = query.where(replication_key_col >= start_val)

        result = self.connector.connection.execute(query)
        for row in result.mappings():  # SQLAlchemy 2.x-friendly
            record = dict(row)
            yield conform_record_data_types(
                stream_name=self.name,
                row=record,
                schema=self.schema,
                logger=self.logger,
            )
