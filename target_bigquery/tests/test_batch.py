"""Tests the Batch Sink"""
from collections import namedtuple
from unittest.mock import patch

from ..target import TargetBigQuery
from . import test_utils

MINIMAL_CONFIG = {
    "project_id": "projid",
    "dataset": "dataid",
    "add_record_metadata": False,
}

TRUNCATE_CONFIG = {
    "project_id": "projid",
    "dataset": "dataid",
    "add_record_metadata": False,
    "truncate_before_load": True,
}

TRUNCATE_TABLE_CONFIG = {
    "project_id": "projid",
    "dataset": "dataid",
    "add_record_metadata": False,
    "table_configs": [
        {
            "table_name": "test_stream",
            "truncate_before_load": True,
        },
    ],
}

APPEND_TABLE_CONFIG = {
    "project_id": "projid",
    "dataset": "dataid",
    "add_record_metadata": False,
    "table_configs": [
        {
            "table_name": "test_stream",
            "append_only": True,
        },
    ],
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
        assert "MERGE `dataid`.`test_stream`" in current_argument
        assert "DROP TABLE `dataid`.`test_stream_" in current_argument

    # Awaiting job
    mock_client.return_value.query.return_value.result.assert_called_once()


@patch("target_bigquery.bq.Client", autospec=True)
def test_loads_records_with_truncate_config(mock_client):
    """Non-batch aware tap with truncate config"""
    target = TargetBigQuery(config=TRUNCATE_CONFIG)

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
    mock_client.return_value.load_table_from_file.return_value.result.assert_called_once()  # noqa: E501

    # Real table is created without expiration
    second_table_kwargs = mock_client.return_value.create_table.call_args_list[1].kwargs
    assert second_table_kwargs["table"].expires is None

    # Finish with merging data and dropping temp table
    mock_client.return_value.query.assert_called()

    for args in mock_client.return_value.query.call_args_list:
        current_argument = args[0][0]
        assert isinstance(current_argument, str)
        assert "TRUNCATE TABLE `dataid`.`test_stream`" in current_argument
        assert "INSERT INTO `dataid`.`test_stream`" in current_argument
        assert "DROP TABLE `dataid`.`test_stream_" in current_argument

    # Awaiting job
    mock_client.return_value.query.return_value.result.assert_called()


@patch("target_bigquery.bq.Client", autospec=True)
def test_loads_records_with_table_truncate_config(mock_client):
    """Non-batch aware tap with truncate config"""
    target = TargetBigQuery(config=TRUNCATE_TABLE_CONFIG)

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
    mock_client.return_value.load_table_from_file.return_value.result.assert_called_once()  # noqa: E501

    # Real table is created without expiration
    second_table_kwargs = mock_client.return_value.create_table.call_args_list[1].kwargs
    assert second_table_kwargs["table"].expires is None

    # Finish with merging data and dropping temp table
    mock_client.return_value.query.assert_called_once()

    for args in mock_client.return_value.query.call_args_list:
        current_argument = args[0][0]
        assert isinstance(current_argument, str)
        assert "TRUNCATE TABLE `dataid`.`test_stream`" in current_argument
        assert "INSERT INTO `dataid`.`test_stream`" in current_argument
        assert "DROP TABLE `dataid`.`test_stream_" in current_argument

    # Awaiting job
    mock_client.return_value.query.return_value.result.assert_called_once()


@patch("target_bigquery.bq.Client", autospec=True)
def test_loads_records_with_table_append_config(mock_client):
    """Non-batch aware tap with truncate config"""
    target = TargetBigQuery(config=APPEND_TABLE_CONFIG)

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
        assert "INSERT INTO `dataid`.`test_stream`" in current_argument
        assert "DROP TABLE `dataid`.`test_stream_" in current_argument
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
    mock_client.return_value.load_table_from_file.return_value.result.assert_called_once()  # noqa: E501

    # Real table is created without expiration
    second_table_kwargs = mock_client.return_value.create_table.call_args_list[1].kwargs
    assert second_table_kwargs["table"].expires is None

    # Finish with merging data and dropping temp table
    mock_client.return_value.query.assert_called()

    for args in mock_client.return_value.query.call_args_list:
        current_argument = args[0][0]
        assert isinstance(current_argument, str)
        assert "TRUNCATE" not in current_argument
        assert "MERGE `dataid`.`batch_test`" in current_argument
        assert "DROP TABLE `dataid`.`batch_test_" in current_argument
    # Awaiting job
    mock_client.return_value.query.return_value.result.assert_called()


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
    # clustering field
    assert second_table_kwargs["table"].clustering_fields is not None

    # Finish with merging data and dropping temp table
    mock_client.return_value.query.assert_called()

    for args in mock_client.return_value.query.call_args_list:
        current_argument = args[0][0]
        assert isinstance(current_argument, str)
        assert "TRUNCATE" not in current_argument
        assert "MERGE `dataid`.`batch_test`" in current_argument
        assert "DROP TABLE `dataid`.`batch_test_" in current_argument

    # Awaiting job
    mock_client.return_value.query.return_value.result.assert_called()


@patch("target_bigquery.bq.Client", autospec=True)
def test_loads_batch_three_no_primary_key(mock_client):
    """Non-batch aware tap with minimal config"""
    target = TargetBigQuery(config=MINIMAL_CONFIG)

    tap_lines = test_utils.get_test_tap_lines("batch_nopq_three.jsonl")

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
    # No clustering field because no primary key
    assert second_table_kwargs["table"].clustering_fields is None

    # Finish with merging data and dropping temp table
    mock_client.return_value.query.assert_called()

    for args in mock_client.return_value.query.call_args_list:
        current_argument = args[0][0]
        assert isinstance(current_argument, str)
        assert "TRUNCATE" not in current_argument
        assert "INSERT INTO `dataid`.`batch_test`" in current_argument
        assert "DROP TABLE `dataid`.`batch_test_" in current_argument

    # Awaiting job
    mock_client.return_value.query.return_value.result.assert_called()


@patch("target_bigquery.bq.Client", autospec=True)
def test_loads_batch_three_no_primary_key_with_truncate(mock_client):
    """Non-batch aware tap with minimal config"""
    target = TargetBigQuery(config=TRUNCATE_CONFIG)

    tap_lines = test_utils.get_test_tap_lines("batch_nopq_three.jsonl")

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
    # No clustering field because no primary key
    assert second_table_kwargs["table"].clustering_fields is None

    # Finish with merging data and dropping temp table
    mock_client.return_value.query.assert_called()
    for i, args in enumerate(mock_client.return_value.query.call_args_list):
        current_argument = args[0][0]
        assert isinstance(current_argument, str)
        if i == 0:
            assert "TRUNCATE TABLE `dataid`.`batch_test`" in current_argument
            assert "INSERT INTO `dataid`.`batch_test`" in current_argument
            assert "DROP TABLE `dataid`.`batch_test_" in current_argument
        else:
            assert "TRUNCATE" not in current_argument
            assert "INSERT INTO `dataid`.`batch_test`" in current_argument
            assert "DROP TABLE `dataid`.`batch_test_" in current_argument

    # Awaiting job
    mock_client.return_value.query.return_value.result.assert_called()


@patch("target_bigquery.bq.Client", autospec=True)
def test_creates_missing_columns_for_existing_tables(mock_client):
    Field = namedtuple("Field", ["name"])

    mock_client.return_value.get_table.return_value.schema = [
        Field("c_pk"),
        Field("c_varchar"),
    ]

    target = TargetBigQuery(config=MINIMAL_CONFIG)

    tap_lines = test_utils.get_test_tap_lines("records_one_stream.jsonl")

    target.listen(file_input=tap_lines)

    mock_client.return_value.update_table.assert_called_once()


@patch("target_bigquery.bq.Client", autospec=True)
def test_creates_does_not_create_columns_when_all_there(mock_client):
    Field = namedtuple("Field", ["name"])

    mock_client.return_value.get_table.return_value.schema = [
        Field("c_pk"),
        Field("c_varchar"),
        Field("c_int"),
    ]

    target = TargetBigQuery(config=MINIMAL_CONFIG)

    tap_lines = test_utils.get_test_tap_lines("records_one_stream.jsonl")

    target.listen(file_input=tap_lines)

    mock_client.return_value.update_table.assert_not_called()
