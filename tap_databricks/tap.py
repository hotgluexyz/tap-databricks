"""databricks tap class."""

from __future__ import annotations

from typing import Any

import requests
from hotglue_etl_exceptions import InvalidCredentialsError
from hotglue_singer_sdk import Stream, Tap
from hotglue_singer_sdk import typing as th  # JSON schema typing helpers
from hotglue_singer_sdk.authenticators import OAuthAuthenticator
from typing_extensions import override

from tap_databricks.auth import databricksAuthenticator
from tap_databricks.client import DatabricksConnector
from tap_databricks.streams import DynamicStream
from tap_databricks.utils import _uc_table_schema, build_catalog_entry_from_uc, tap_stream_id


class Tapdatabricks(Tap):
    """Singer tap for databricks."""

    name = "tap-databricks"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "oauth_scope",
            th.StringType,
            default="all-apis",
            description="OAuth scope for service principal token (typically all-apis)",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
            default="2000-01-01T00:00:00Z",
        ),
        th.Property(
            "host",
            th.StringType,
            required=True,
            description="Databricks host (e.g. dbc-xxxx.cloud.databricks.com)",
        ),
        th.Property(
            "client_id",
            th.StringType,
            required=True,
            description="OAuth client ID for the databricks OAuth app",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=True,
            description="OAuth client secret for the databricks OAuth app",
        ),
        th.Property(
            "http_path",
            th.StringType,
            required=True,
            description="Databricks http path to use for the sync (e.g. /sql/1.0/warehouses/warehouse_id)",
        ),
        th.Property(
            "tables",
            th.StringType,
            required=False,
            description="Comma-separated list of tables to sync",
        ),
        th.Property(
            "catalog",
            th.StringType,
            required=False,
            description="Databricks catalog to use for the sync",
        ),
        th.Property(
            "default_target_schema",
            th.StringType,
            required=False,
            description="Databricks schema to use for the sync",
        ),
        th.Property(
            "table_selection",
            th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType),
                    th.Property("replication_key", th.StringType),
                )
            ),
            required=False,
            description="List of tables, that belong to the catalog and schema, to sync",
        ),
    ).to_dict()

    def _uc_get(self, path: str, params: dict | None = None) -> dict:
        """GET a Unity Catalog API endpoint."""
        auth_cls, endpoint = self.access_token_support(self)
        self.update_access_token(auth_cls, endpoint, self)
        url = f"https://{self.config['host']}{path}"
        response = requests.get(
            url,
            headers={"Authorization": f"Bearer {self.config['access_token']}"},
            params=params or {},
            timeout=30,
        )
        if response.status_code == 403:
            raise InvalidCredentialsError(f"Permission denied calling {path}: {response.text}")
        response.raise_for_status()
        return response.json()

    def _merge_primary_keys(
        self,
        table: dict,
        table_selection: list[dict] | None,
    ) -> list[str]:
        keys: list[str] = []
        seen: set[str] = set()

        for constraint in table.get("table_constraints") or []:
            for column in (constraint.get("primary_key_constraint") or {}).get(
                "child_columns"
            ) or []:
                if column not in seen:
                    seen.add(column)
                    keys.append(column)

        for entry in table_selection or []:
            if entry.get("name") != table.get("name"):
                continue
            pk = entry.get("primary_key")
            if not pk:
                continue
            for column in [pk] if isinstance(pk, str) else pk:
                if column not in seen:
                    seen.add(column)
                    keys.append(column)

        return keys

    @override
    def discover_streams(self) -> list[Stream]:
        """Using the Unity Catalog endpoint, return a dynamically discovered Catalog describing the structure of the database."""
        streams: list[Stream] = []
        config_table_selection = None
        config_selected_tables = None
        config_tables = self.config.get("tables")
        config_catalog = self.config.get("catalog")
        config_schema = self.config.get(
            "default_target_schema"
        )  # it's the name of the schema in the target, we need it to implement bidirectional flows
        config_table_selection = self.config.get("table_selection")
        if self._input_catalog:
            # on sync there is no need to discover unselected streams
            config_selected_tables = [
                entry.tap_stream_id
                for entry in self._input_catalog.streams
                if entry.metadata.resolve_selection().get((), False)
            ]
        elif config_tables:
            # "tables": "MYDB.MYSCHEMA.Table1,MYDB.MYSCHEMA.Table2"
            config_selected_tables = [
                config_table.strip() for config_table in str(config_tables).split(",")
            ]
        elif config_catalog and config_schema and config_table_selection:
            # we need to build it up database.schema.table
            config_selected_tables = [
                f"{config_catalog}.{config_schema}.{t.get('name')}" for t in config_table_selection
            ]

        connector = DatabricksConnector(dict(self.config))
        uc_catalogs = self._uc_get("/api/2.1/unity-catalog/catalogs").get("catalogs", [])
        for catalog in uc_catalogs:
            catalog_name = catalog["name"]
            if (
                config_selected_tables is not None
                and catalog_name not in [t.split(".")[0] for t in config_selected_tables]
            ) or (
                not config_tables
                and self.config.get("catalog", "") != ""
                and catalog_name != self.config.get("catalog")
            ):
                # skip this catalog
                continue
            uc_schemas = self._uc_get(
                "/api/2.1/unity-catalog/schemas",
                {"catalog_name": catalog_name},
            ).get("schemas", [])
            for schema in uc_schemas:
                schema_name = schema["name"]
                if (
                    config_selected_tables
                    and f"{catalog_name}.{schema_name}"
                    not in [".".join(t.split(".")[:2]) for t in config_selected_tables]
                ) or (
                    not config_tables
                    and self.config.get("default_target_schema", "") != ""
                    and schema_name != self.config.get("default_target_schema")
                ):
                    # skip this catalog.schema
                    continue
                uc_tables = self._uc_get(
                    "/api/2.1/unity-catalog/tables",
                    {"catalog_name": catalog_name, "schema_name": schema_name},
                ).get("tables", [])
                for table in uc_tables:
                    table_name = table["name"]
                    if (
                        config_selected_tables
                        and f"{catalog_name}.{schema_name}.{table_name}"
                        not in config_selected_tables
                    ):
                        # skip this catalog.schema.table if it's not in the config_selected_tables
                        continue
                    table = self._uc_get(
                        f"/api/2.1/unity-catalog/tables/{catalog_name}.{schema_name}.{table_name}"
                    )
                    ##replication key can come from the config_table_selection.primary_key and the uc_tables
                    replication_key = next(
                        (
                            entry.get("replication_key")
                            for entry in (config_table_selection or [])
                            if entry.get("name") == table_name and entry.get("replication_key")
                        ),
                        None,
                    )
                    primary_keys = self._merge_primary_keys(table, config_table_selection)
                    schema_dict, unsupported = _uc_table_schema(table.get("columns", []))
                    entry = build_catalog_entry_from_uc(
                        uc_catalog_name=catalog_name,
                        uc_schema_name=schema_name,
                        uc_table_name=table_name,
                        schema_dict=schema_dict,
                        table_meta=table,
                        unsupported_columns=unsupported,
                        replication_key=replication_key,
                        primary_keys=primary_keys,
                    )
                    connector.register_table(
                        tap_stream_id(catalog_name, schema_name, table_name), schema_dict
                    )
                    streams.append(
                        DynamicStream(
                            tap=self,
                            catalog_entry=entry,
                            connector=connector,
                        )
                    )
        return streams

    @classmethod
    def access_token_support(  # type: ignore[override]  # ty: ignore[invalid-method-override]
        cls,
        connector: Any = None,
    ) -> tuple[type[OAuthAuthenticator], str]:
        """Return the authenticator class and OAuth token endpoint.

        Returns:
            A tuple with the authenticator class and the OAuth token endpoint URL.
        """
        host = (connector.config if connector else {}).get("host", "")
        return databricksAuthenticator, f"https://{host}/oidc/v1/token"


if __name__ == "__main__":
    Tapdatabricks.cli()
