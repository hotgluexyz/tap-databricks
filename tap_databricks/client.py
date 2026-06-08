"""Databricks SQL connector for SQLStream sync."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

import sqlalchemy
from databricks.sdk.core import Config, oauth_service_principal
from hotglue_singer_sdk.streams.sql import SQLConnector
from sqlalchemy.engine import Engine
from typing_extensions import override


class DatabricksConnector(SQLConnector):
    """SQLAlchemy connector for Databricks SQL warehouses with UC table cache."""

    def __init__(self, config: dict | None = None, sqlalchemy_url: str | None = None) -> None:
        super().__init__(config=config, sqlalchemy_url=sqlalchemy_url)
        self._table_schemas: dict[str, dict] = {}

    def register_table(self, full_table_name: str, schema_dict: dict) -> None:
        """Cache JSON Schema from Unity Catalog discovery for sync."""
        self._table_schemas[full_table_name] = schema_dict

    @override
    def get_sqlalchemy_url(self, config: dict[str, Any]) -> str:
        warehouse = config["warehouse"]
        api_url = config["api_url"]
        host = urlparse(str(api_url).rstrip("/")).hostname
        http_path = f"/sql/1.0/warehouses/{warehouse}"
        return f"databricks://token:dummy@{host}?http_path={http_path}"

    def _credential_provider(self):
        config = Config(
            host=self.config["api_url"].rstrip("/"),
            client_id=self.config["client_id"],
            client_secret=self.config["client_secret"],
        )
        return oauth_service_principal(config)

    @override
    def create_sqlalchemy_engine(self) -> Engine:
        return sqlalchemy.create_engine(
            self.sqlalchemy_url,
            echo=False,
            connect_args={"credentials_provider": self._credential_provider},
        )

    @override
    def get_table_columns(self, full_table_name: str) -> dict[str, sqlalchemy.Column]:
        schema_dict = self._table_schemas.get(full_table_name)
        if schema_dict is None:
            raise KeyError(f"No cached schema for {full_table_name!r}. Run discover before sync.")
        result = {}
        for name, prop in schema_dict.get("properties", {}).items():
            if "description" in prop and "type" not in prop:
                continue
            result[name] = sqlalchemy.Column(
                name,
                self.to_sql_type(prop),
                nullable="null" in (prop.get("type") if isinstance(prop.get("type"), list) else []),
            )
        return result

    @override
    def get_table(self, full_table_name: str) -> sqlalchemy.Table:
        catalog, schema, table_name = self.parse_full_table_name(full_table_name)
        columns = self.get_table_columns(full_table_name).values()
        meta = sqlalchemy.MetaData()
        qualified_schema = f"{catalog}.{schema}"
        return sqlalchemy.Table(
            table_name,
            meta,
            *list(columns),
            schema=qualified_schema,
            quote_schema=False,
        )
