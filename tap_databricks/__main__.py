"""databricks entry point."""

from __future__ import annotations

from tap_databricks.tap import Tapdatabricks

Tapdatabricks.cli()
