"""Tests the Batch Sink"""
from ...target import TargetBigQuery
from .. import test_utils
from .int_utils import get_client
from datetime import datetime


def test_loads_records_with_minimal_config():
    """Non-batch aware tap with minimal config"""
    target = TargetBigQuery(config = {}, parse_env_config=True)

    tap_lines = test_utils.get_test_tap_lines('batch_three.jsonl')

    start = datetime.now()

    target.listen(tap_lines)

    elapsed = datetime.now() - start

    print(f"Test took {elapsed}")

    assert(False)
