"""Tests the Batch Sink"""
from unittest.mock import Mock

from ..target import TargetBigQuery
from . import test_utils


MINIMAL_CONFIG = {
    "project_id": "projid",
    "dataset": "dataid",
}

# def test_loads_records_with_minimal_config():
#     """Non-batch aware tap with minimal config"""
#     target = TargetBigQuery(config = MINIMAL_CONFIG)

#     target.get_client = Mock()

#     tap_lines = test_utils.get_test_tap_lines('records_one_stream.jsonl')

#     target._process_lines(tap_lines)

    #target.get_client.assert_called_once_with(project_id="projid", location=None)
    #target.get_client.return_value.dataset.assert_called_with('dataid')
