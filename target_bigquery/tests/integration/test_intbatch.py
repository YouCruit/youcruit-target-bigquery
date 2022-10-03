"""Tests the Batch Sink"""
# from unittest.mock import Mock

# from ...target import TargetBigQuery
# from .. import test_utils
# from .int_utils import get_client


# def test_loads_records_with_minimal_config():
#     """Non-batch aware tap with minimal config"""
#     target = TargetBigQuery(config = {}, parse_env_config=True)

#     tap_lines = test_utils.get_test_tap_lines('records_one_stream.jsonl')

#     target._process_lines(tap_lines)
#     target._process_endofpipe()

#     #query(tablename, column),
#     #setofvalues[]

#     assert(False)
