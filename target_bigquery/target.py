"""BigQuery target class."""

from __future__ import annotations
from pathlib import PurePath
from pydoc import describe
from typing import List, Optional, Union

from singer_sdk.target_base import Target
from singer_sdk import typing as th
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
        # th.Property(
        #     "stream_maps",
        #     th.ObjectType,
        #     description="Define an optional transform of the incoming data",
        # ),
        # th.Property(
        #     "stream_map_config",
        #     th.ObjectType,
        #     description="Optional config options be used inside the stream map",
        # ),
    ).to_dict()

    default_sink_class = BigQuerySink

    def get_client(self, project_id: str, location: str = None) -> bigquery.Client:
        """Returns a Google Client. This is a method so it can be mocked in tests."""
        return bigquery.Client(project=project_id, location=location)


if __name__ == "__main__":
    TargetBigQuery.cli()
