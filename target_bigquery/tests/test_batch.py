"""Tests the Batch Sink"""
from unittest.mock import patch

from ..target import TargetBigQuery
from . import test_utils


MINIMAL_CONFIG = {
    "project_id": "projid",
    "dataset": "dataid",
}


@patch("target_bigquery.bq.Client", autospec=True)
def test_loads_records_with_minimal_config(mock_client):
    """Non-batch aware tap with minimal config"""
    target = TargetBigQuery(config=MINIMAL_CONFIG)

    tap_lines = test_utils.get_test_tap_lines("records_one_stream.jsonl")

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
    mock_client.return_value.load_table_from_file.return_value.result.assert_called_once()

    # Real table is created without expiration
    second_table_kwargs = mock_client.return_value.create_table.call_args_list[1].kwargs
    assert second_table_kwargs["table"].expires is None

    # Finish with merging data and dropping temp table
    mock_client.return_value.query.assert_called_once()
    assert mock_client.return_value.query.call_args_list[0].startswith(
        "MERGE `dataid`.`test_stream`"
    )
    assert mock_client.return_value.query.call_args_list[0].contains(
        "DROP TABLE `dataid`.`test_stream_"
    )
    # Awaiting job
    mock_client.return_value.query.return_value.result.assert_called_once()


@patch("target_bigquery.bq.Client", autospec=True)
def test_loads_batch_one(mock_client):
    """Non-batch aware tap with minimal config"""
    target = TargetBigQuery(config=MINIMAL_CONFIG)

    tap_lines = test_utils.get_test_tap_lines("batch_one.jsonl")

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
    mock_client.return_value.load_table_from_file.return_value.result.assert_called_once()

    # Real table is created without expiration
    second_table_kwargs = mock_client.return_value.create_table.call_args_list[1].kwargs
    assert second_table_kwargs["table"].expires is None

    # Finish with merging data and dropping temp table
    mock_client.return_value.query.assert_called_once()
    assert mock_client.return_value.query.call_args_list[0].startswith(
        "MERGE `dataid`.`test_stream`"
    )
    assert mock_client.return_value.query.call_args_list[0].contains(
        "DROP TABLE `dataid`.`test_stream_"
    )
    # Awaiting job
    mock_client.return_value.query.return_value.result.assert_called_once()


@patch("target_bigquery.bq.Client", autospec=True)
def test_loads_batch_three(mock_client):
    """Non-batch aware tap with minimal config"""
    target = TargetBigQuery(config=MINIMAL_CONFIG)

    tap_lines = test_utils.get_test_tap_lines("batch_three.jsonl")

    target.listen(file_input=tap_lines)
    # target._process_lines(tap_lines)
    # target._process_endofpipe()

    mock_client.assert_called_once_with(project="projid", location=None)

    # Temporary table is created with expiration
    first_table_kwargs = mock_client.return_value.create_table.call_args_list[0].kwargs
    assert first_table_kwargs["table"].expires is not None

    # Data is loaded into table
    mock_client.return_value.load_table_from_file.assert_called()
    # Job is awaited
    mock_client.return_value.load_table_from_file.return_value.result.assert_called()

    # Real table is created without expiration
    second_table_kwargs = mock_client.return_value.create_table.call_args_list[1].kwargs
    assert second_table_kwargs["table"].expires is None

    # Finish with merging data and dropping temp table
    mock_client.return_value.query.assert_called()
    assert mock_client.return_value.query.call_args_list[0].startswith(
        "MERGE `dataid`.`test_stream`"
    )
    assert mock_client.return_value.query.call_args_list[0].contains(
        "DROP TABLE `dataid`.`test_stream_"
    )
    # Awaiting job
    mock_client.return_value.query.return_value.result.assert_called()
