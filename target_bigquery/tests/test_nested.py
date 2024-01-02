"""Tests the Batch Sink"""
from unittest.mock import patch

from ..target import TargetBigQuery
from . import test_utils

MINIMAL_CONFIG = {"project_id": "projid", "dataset": "dataid"}


@patch("target_bigquery.bq.Client", autospec=True)
def test_loads_nested_record(mock_client):
    """Non-batch aware tap with minimal config"""
    target = TargetBigQuery(config=MINIMAL_CONFIG)

    tap_lines = test_utils.get_test_tap_lines("nested.jsonl")

    target.listen(file_input=tap_lines)

    # target._process_lines(tap_lines)
    # target._process_endofpipe()

    mock_client.assert_called_once_with(project="projid", location=None)

    # Temporary table is created with expiration
    first_table_kwargs = mock_client.return_value.create_table.call_args_list[0].kwargs
    assert first_table_kwargs["table"].expires is not None

    # Data is loaded into table
    mock_client.return_value.load_table_from_file.assert_called_once()
    # Job is awaited
    mock_client.return_value.load_table_from_file.return_value.result.assert_called_once()  # noqa: E501

    # Real table is created without expiration
    second_table_kwargs = mock_client.return_value.create_table.call_args_list[1].kwargs
    assert second_table_kwargs["table"].expires is None

    # Finish with merging data and dropping temp table
    mock_client.return_value.query.assert_called_once()

    for args in mock_client.return_value.query.call_args_list:
        current_argument = args[0][0]
        assert isinstance(current_argument, str)
        assert "TRUNCATE" not in current_argument
        assert "MERGE `dataid`.`transactions`" in current_argument
        assert "DROP TABLE `dataid`.`transactions" in current_argument

    # Awaiting job
    mock_client.return_value.query.return_value.result.assert_called_once()
