"""BigQuery target class."""

from __future__ import annotations
from pathlib import PurePath
from pydoc import describe
from typing import List, Optional, Union

from singer_sdk.target_base import Target
from singer_sdk import typing as th
from singer_sdk.helpers._batch import BaseBatchFileEncoding
from google.cloud import bigquery

from .sinks import (
    BigQuerySink,
)


class TargetBigQuery(Target):
    """Sample target for BigQuery."""

    name = "target-bigquery"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "project_id",
            th.StringType,
            description="Google project id",
            required=True,
        ),
        th.Property(
            "dataset",
            th.StringType,
            description="Dataset to load data into",
            required=True,
        ),
        th.Property(
            "location",
            th.StringType,
            description="Dataset location",
            required=False,
            default=None,
        ),
        th.Property(
            "table_prefix",
            th.StringType,
            description="Optional prefix to add to table names",
            required=False,
            default=None,
        ),
    ).to_dict()

    default_sink_class = BigQuerySink

    def _process_batch_message(self, message_dict: dict) -> None:
        """Overridden because Meltano 0.11.1 has a bad implementation see
        https://github.com/meltano/sdk/issues/1031
        """
        sink = self.get_sink(message_dict["stream"])

        encoding = BaseBatchFileEncoding.from_dict(message_dict["encoding"])
        sink.process_batch_files(
            encoding,
            message_dict["manifest"],
        )


if __name__ == "__main__":
    TargetBigQuery.cli()
