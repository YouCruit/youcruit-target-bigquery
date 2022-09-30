"""BigQuery target class."""

from __future__ import annotations
from pathlib import PurePath
from typing import List, Optional, Union

from singer_sdk.target_base import Target
from singer_sdk import typing as th

from target_bigquery.sinks import (
    BigQuerySink,
)


class TargetBigQuery(Target):
    """Sample target for BigQuery."""

    name = "target-bigquery"
    config_jsonschema = th.PropertiesList(
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


if __name__ == "__main__":
    TargetBigQuery.cli()
