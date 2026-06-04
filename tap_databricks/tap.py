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
from tap_databricks.streams import UnityCatalogTableStream

_INTEGER_TYPES = {"INT", "SHORT", "BYTE"}
_NUMBER_TYPES = {"LONG", "FLOAT", "DOUBLE", "DECIMAL"}
_DATETIME_TYPES = {"TIMESTAMP"}


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
            "api_url",
            th.StringType,
            required=True,
            description="Databricks workspace URL (e.g. https://dbc-xxxx.cloud.databricks.com)",
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
    ).to_dict()

    def _uc_get(self, path: str, params: dict | None = None) -> dict:
        """GET a Unity Catalog API endpoint."""
        auth_cls, endpoint = self.access_token_support(self)
        self.update_access_token(auth_cls, endpoint, self)
        url = f"{self.config['api_url'].rstrip('/')}{path}"
        response = requests.get(
            url,
            headers={"Authorization": f"Bearer {self.config['access_token']}"},
            params=params or {},
            timeout=30,
        )
        if response.status_code == 403:
            raise InvalidCredentialsError(
                f"Permission denied calling {path}: {response.text}"
            )
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
            for column in (constraint.get("primary_key_constraint") or {}).get("child_columns") or []:
                if column not in seen:
                    seen.add(column)
                    keys.append(column)

        for entry in table_selection or []:
            if entry.get("name") != table.get("name"):
                continue
            pk = entry.get("primary_key")
            if not pk:
                continue
            for column in ([pk] if isinstance(pk, str) else pk):
                if column not in seen:
                    seen.add(column)
                    keys.append(column)
        
        return keys

    @override
    def discover_streams(self) -> list[Stream]:
        """Using the Unity Catalog endpoint, return a dynamically discovered Catalog describing the structure of the database."""
        streams: list[Stream] = []
        table_selection = None
        config_selected_tables = None
        if self.config.get('tables'):
            #"tables": "MYDB.MYSCHEMA.Table1,MYDB.MYSCHEMA.Table2"
            config_selected_tables = [
                table.strip()
                for table in self.config.get("tables").split(",")
            ]
        elif self.config.get('table_selection'):
            catalog = self.config.get('catalog')
            schema = self.config.get('schema')
            table_selection = self.config.get('table_selection')
            # we need to build it up database.schema.table
            config_selected_tables = [f"{catalog}.{schema}.{t.get('name')}" for t in table_selection]

        
        uc_catalogs = self._uc_get("/api/2.1/unity-catalog/catalogs").get("catalogs", [])
        for catalog in uc_catalogs:
            catalog_name = catalog["name"]
            if (config_selected_tables is not None and catalog_name not in [t.split('.')[0] for t in config_selected_tables]) \
                or (self.config.get('catalog', "") != "" and catalog_name != self.config.get('catalog')):
                # skip this catalog
                continue
            uc_schemas = self._uc_get("/api/2.1/unity-catalog/schemas",{"catalog_name": catalog_name},).get("schemas", [])
            for schema in uc_schemas:
                schema_name = schema["name"]
                if (config_selected_tables and f"{catalog_name}.{schema_name}" not in ['.'.join(t.split('.')[:2]) for t in config_selected_tables])\
                    or (self.config.get('schema', "") != "" and schema_name != self.config.get('schema')):
                    # skip this catalog.schema
                    continue
                uc_tables = self._uc_get("/api/2.1/unity-catalog/tables",{"catalog_name": catalog_name, "schema_name": schema_name},).get("tables", [])
                for table in uc_tables:
                    table_name = table["name"]
                    if config_selected_tables and f"{catalog_name}.{schema_name}.{table_name}" not in config_selected_tables:
                        # skip this catalog.schema.table if it's not in the config_selected_tables
                        continue
                    table = self._uc_get(f"/api/2.1/unity-catalog/tables/{catalog_name}.{schema_name}.{table_name}")
                    ##TODO: handle replication key and primary key
                    ##replication key can come from the config table_selection.primary_key and the table 
                    replication_key = next((entry.get("replication_key") for entry in (table_selection or []) if entry.get("name") == table_name and entry.get("replication_key")), None)
                    primary_keys = self._merge_primary_keys(table, table_selection)
                    schema_dict, unsupported = _uc_table_schema(table.get("columns", []))
                    streams.append(
                        UnityCatalogTableStream(
                            tap=self,
                            catalog_name=catalog_name,
                            schema_name=schema_name,
                            table_name=table_name,
                            schema=schema_dict,
                            table_meta=table,
                            unsupported_columns=unsupported,
                            replication_key=replication_key,
                            primary_keys=primary_keys,
                        )
                    )
        return streams

    @classmethod
    def access_token_support(
        cls,
        connector: Any = None,
    ) -> tuple[type[OAuthAuthenticator], str]:
        """Return the authenticator class and OAuth token endpoint.

        Returns:
            A tuple with the authenticator class and the OAuth token endpoint URL.
        """
        host = (connector.config if connector else {}).get("api_url", "").rstrip("/")
        return databricksAuthenticator, f"{host}/oidc/v1/token"


if __name__ == "__main__":
    Tapdatabricks.cli()
