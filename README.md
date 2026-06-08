# tap-databricks

A [Singer](https://www.singer.io/) tap that extracts data from **Databricks** tables via the Unity Catalog REST API (discovery) and Databricks SQL (sync). Built with [hotglue-singer-sdk](https://github.com/hotgluexyz/HotglueSingerSDK).

## Features

- **Dynamic stream discovery** — one Singer stream per Unity Catalog table (`catalog.schema.table`).
- **OAuth2 service principal** authentication via the Databricks OIDC token endpoint.
- **SQL sync** through a Databricks SQL warehouse using SQLAlchemy and the Databricks SQL connector.
- **Incremental replication** when a `replication_key` is configured per table (via `table_selection`).
- **Flexible table selection** — sync all accessible tables, filter by catalog/schema, pass an explicit table list, or configure per-table replication keys.
- **Schema mapping** from Unity Catalog column types to JSON Schema; unsupported types are marked `unsupported` in the catalog.

### Stream naming

Each stream ID follows:

```
{catalog}.{schema}.{table}
```

Example: `samples.bakehouse.media_customer_reviews`

Replication method is **INCREMENTAL** when a `replication_key` is set for that table; otherwise **FULL_TABLE**. Primary keys come from Unity Catalog constraints and/or `table_selection` overrides.

## Requirements

- Python **3.10+**
- A Databricks workspace
- A service principal with permissions User to the workspace

## Installation

1. Clone this repository and `cd` into the project directory.
2. Create a virtual environment and activate it:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

3. Install the package in editable mode (include dev deps for testing):

```bash
pip install -e ".[dev]"
```

Or with **uv**:

```bash
uv sync --group dev
```

4. Verify the CLI:

```bash
tap-databricks --help
```

## Configuration

Run `tap-databricks --about` (or `tap-databricks --about --format=markdown`) for the authoritative schema.

| Setting | Type | Required | Default | Description |
| ------- | ---- | -------- | ------- | ----------- |
| `host` | string | yes | — | Databricks host (e.g. `dbc-xxxx.cloud.databricks.com`) |
| `client_id` | string | yes | — | OAuth client ID (service principal) |
| `client_secret` | string | yes | — | OAuth client secret |
| `http_path` | string | yes | — | Databricks SQL warehouse HTTP path (e.g. `/sql/1.0/warehouses/{warehouse_id}`) |
| `oauth_scope` | string | no | `all-apis` | OAuth scope for the service principal token |
| `start_date` | datetime | no | `2000-01-01T00:00:00Z` | Earliest replication key value for incremental streams |
| `tables` | string | no | — | Comma-separated fully qualified tables: `catalog.schema.table,...` |
| `catalog` | string | no | — | Limit discovery to this Unity Catalog |
| `schema` | string | no | — | Limit discovery to this schema (within `catalog`) |
| `table_selection` | array | no | — | Per-table config when using `catalog` + `schema` (see below) |

Do not commit real credentials. Use `.secrets/`, environment variables, or a secrets manager locally and in production.

### Table selection

If none of the table-selection settings are provided, the tap discovers **all** Unity Catalog tables the service principal can access.

**Option A — explicit table list** (highest priority):

```json
{
  "tables": "samples.bakehouse.media_customer_reviews,samples.bakehouse.sales_transactions"
}
```

**Option B — catalog + schema** (all tables in that schema):

```json
{
  "catalog": "samples",
  "schema": "bakehouse"
}
```

**Option C — catalog + schema + `table_selection`** (specific tables with replication keys):

```json
{
  "catalog": "samples",
  "schema": "bakehouse",
  "table_selection": [
    {
      "name": "media_customer_reviews",
      "replication_key": "review_date"
    },
    {
      "name": "sales_transactions",
      "replication_key": "transaction_date"
    }
  ]
}
```

`table_selection` entries may also include `primary_key` (string or array) to be added to the UC primary key metadata.

If both `tables` and `table_selection` are set, **`tables` takes precedence**.

### Example `config.json`

```json
{
  "host": "dbc-xxxx.cloud.databricks.com",
  "client_id": "YOUR_CLIENT_ID",
  "client_secret": "YOUR_CLIENT_SECRET",
  "http_path": "/sql/1.0/warehouses/YOUR_WAREHOUSE_ID",
  "oauth_scope": "all-apis",
  "start_date": "2000-01-01T00:00:00Z",
  "catalog": "samples",
  "schema": "bakehouse"
}
```

### Environment-based config

Copy `.env.example` to `.env`, fill in values, and run with `--config=ENV`:

```bash
cp .env.example .env
tap-databricks --config=ENV --discover > catalog.json
```

Environment variables use the prefix `TAP_DATABRICKS_` + the setting key in uppercase (e.g. `TAP_DATABRICKS_HTTP_PATH`). For `table_selection`, prefer `config.json` over `.env` because it is a JSON array.

## Usage

Discover the stream catalog:

```bash
tap-databricks --config config.json --discover > catalog.json
```

Run a sync (with optional state):

```bash
tap-databricks --config config.json --catalog catalog.json --state state.json
```

Pipe to any Singer target:

```bash
tap-databricks --config config.json --catalog catalog.json | target-jsonl
```

Validate Singer output (requires [hotglue CLI](https://docs.hotglue.com/cli/singer)):

```bash
hotglue singer validate --dataFilePath data.singer
```

Inspect settings and stream metadata:

```bash
tap-databricks --about
```

### Meltano

This project includes a `meltano.yml` for local development. Configure required settings via `meltano config tap-databricks set` or a `.env` file, then:

```bash
meltano install
meltano invoke tap-databricks --discover
meltano run tap-databricks target-jsonl
```

## How it works

1. **Discover** — The tap calls Unity Catalog APIs to list catalogs, schemas, and tables, maps column types to JSON Schema, and builds one `DynamicStream` per table.
2. **Sync** — For each selected stream, `DynamicStream.get_records` runs a SQL `SELECT` against the table via the configured `http_path`. Incremental streams add `ORDER BY` and `WHERE replication_key >= bookmark`.

Supported Unity Catalog types include `STRING`, integer/number types, `BOOLEAN`, `DATE`, `TIMESTAMP`, and `BINARY`. Other types (e.g. `GEOGRAPHY`) are included in discovery as unsupported columns.

## Development

### Project layout

| Path | Purpose |
| ---- | ------- |
| `tap_databricks/tap.py` | Tap class, UC discovery, config schema |
| `tap_databricks/streams.py` | `DynamicStream` — SQL record extraction |
| `tap_databricks/client.py` | `DatabricksConnector` — SQLAlchemy + OAuth |
| `tap_databricks/auth.py` | Service principal OAuth authenticator |
| `tests/` | Unit tests (mocked UC API and SQL) |

### Running tests

```bash
pytest
```

Run a single file:

```bash
pytest tests/test_discovery.py -v
```

Via tox:

```bash
tox -e 3.10
```

Tests mock Unity Catalog HTTP calls and SQL execution — no live Databricks credentials required.

| Test file | Covers |
| --------- | ------ |
| `test_core.py` | SDK standard tap tests (CLI, etc.) |
| `test_discovery.py` | UC schema mapping, catalog entry building, stream discovery |
| `test_sync.py` | SQLAlchemy URL, record extraction, incremental `WHERE` clause |

### Linting and typing

```bash
tox -e lint
tox -e typing
```

## API / documentation

- [Databricks Unity Catalog REST API](https://docs.databricks.com/api/workspace/catalogs/list)
- [Databricks SQL warehouses](https://docs.databricks.com/en/compute/sql-warehouse/)
- [OAuth service principal authentication](https://docs.databricks.com/en/dev-tools/auth/oauth-m2m.html)

## License

Apache 2.0 — see `LICENSE` and `pyproject.toml`.
