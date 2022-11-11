"""Tests the Batch Sink"""
from collections import namedtuple
from unittest.mock import Mock, patch

from ..target import TargetBigQuery
from . import test_utils


MINIMAL_CONFIG = {
    "project_id": "projid",
    "dataset": "dataid",
    "add_record_metadata": False,
    "stream_configs": {
        "test_stream": {
            "time_partition_column": "c_int",
        }
    }
}


@patch("target_bigquery.bq.Client", autospec=True)
def test_creates_partitioned_table(mock_client):
    """Time based partition"""
    target = TargetBigQuery(config=MINIMAL_CONFIG)

    tap_lines = test_utils.get_test_tap_lines("records_one_stream.jsonl")

    target.listen(file_input=tap_lines)

    mock_client.assert_called_once_with(project="projid", location=None)

    # Temporary table is created without time partitioning
    first_table_kwargs = mock_client.return_value.create_table.call_args_list[0].kwargs
    assert first_table_kwargs["table"].expires is not None
    assert first_table_kwargs["table"].time_partitioning.field == "c_int"
    assert first_table_kwargs["table"].time_partitioning.type_ == "DAY"

    # Data is loaded into table
    mock_client.return_value.load_table_from_file.assert_called_once()
    # Job is awaited
    mock_client.return_value.load_table_from_file.return_value.result.assert_called_once()

    # Real table is created with time partition
    real_table_kwargs = mock_client.return_value.create_table.call_args_list[1].kwargs
    assert real_table_kwargs["table"].expires is None
    assert first_table_kwargs["table"].time_partitioning.field == "c_int"
    assert first_table_kwargs["table"].time_partitioning.type_ == "DAY"
