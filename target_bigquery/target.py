"""BigQuery target class."""

from __future__ import annotations
from pathlib import PurePath
from pydoc import describe
from typing import List, Optional, Type, Union

from singer_sdk.target_base import Target
from singer_sdk import typing as th
from singer_sdk.helpers._batch import BaseBatchFileEncoding
from singer_sdk.sinks import Sink

from .sinks import (
    BigQueryAsyncSink,
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
            description="Location of the dataset",
            required=False,
            default=None,
        ),
        th.Property(
            "table_prefix",
            th.StringType,
            description="Prefix of destination table name",
            required=False,
            default=None,
        ),
         th.Property(
            "mode",
            th.StringType,
            description="If 'ASYNC' then files are loaded in parallel",
            required=False,
            default='DEFAULT',
        ),
    ).to_dict()

    def get_sink_class(self, stream_name: str) -> Type[Sink]:
        if self.config.get('mode', 'DEFAULT').lower() == 'async':
            return BigQueryAsyncSink
        else:
            return BigQuerySink

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
