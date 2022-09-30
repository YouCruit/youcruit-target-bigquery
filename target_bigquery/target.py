"""BigQuery target class."""

from __future__ import annotations

from singer_sdk.target_base import Target
from singer_sdk import typing as th

from target_bigquery.sinks import (
    BigQuerySink,
)


class TargetBigQuery(Target):
    """Sample target for BigQuery."""

    name = "target-bigquery"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "filepath",
            th.StringType,
            description="The path to the target output file"
        ),
        th.Property(
            "file_naming_scheme",
            th.StringType,
            description="The scheme with which output files will be named"
        ),
    ).to_dict()

    default_sink_class = BigQuerySink


if __name__ == "__main__":
    TargetBigQuery.cli()
