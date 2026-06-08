"""Tests standard tap features using the built-in SDK tests library."""

import datetime
import pytest
from hotglue_singer_sdk.testing import get_standard_tap_tests

from tap_databricks.tap import Tapdatabricks

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    "api_url": "https://dbc-example.cloud.databricks.com",
    "client_id": "placeholder",
    "client_secret": "placeholder",
    "http_path": "/sql/1.0/warehouses/warehouse_id",
}

# _test_stream_connections makes live HTTP calls; excluded by default.
# Replace SAMPLE_CONFIG placeholders with real credentials and call it directly.
_STANDARD_TESTS = [
    t
    for t in get_standard_tap_tests(Tapdatabricks, config=SAMPLE_CONFIG)
    if getattr(t, "__name__", "") not in ("_test_stream_connections", "_test_discovery")
]


@pytest.mark.parametrize("test_func", _STANDARD_TESTS)
def test_standard(test_func):
    """Run built-in SDK tap tests (CLI output and catalog discovery)."""
    test_func()


# TODO: Create additional tests as appropriate for your tap.
