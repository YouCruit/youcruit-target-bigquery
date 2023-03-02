"""BigQuery target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.helpers._batch import BaseBatchFileEncoding
from singer_sdk.target_base import Target

from .sinks import BigQuerySink


class TargetBigQuery(Target):
    """Sample target for BigQuery."""

    batch_msg_processed: bool = False

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
        th.Property(
            "batch_size",
            th.IntegerType,
            description="Maximum size of batches when records are streamed in. BATCH messages are not affected by this property.",  # noqa: E501
            required=False,
            default=100000,
        ),
        th.Property(
            "max_batch_age",
            th.NumberType,
            description="Maximum time in minutes between state messages when records are streamed in. BATCH messages are not affected by this property.",  # noqa: E501
            required=False,
            default=5.0,
        ),
        th.Property(
            "add_record_metadata",
            th.BooleanType,
            description="Add Singer Data Capture (SDC) metadata to records",
            required=False,
            default=True,
        ),
        th.Property(
            "default_partition_column",
            th.StringType,
            description="Default partition column for all streams",
            required=False,
            default=None,
        ),
        th.Property(
            "truncate_before_load",
            th.BooleanType,
            description="If tables should be truncated before new data is loaded",
            required=False,
            default=False,
        ),
        th.Property(
            "append_only",
            th.BooleanType,
            description="Only append data; don't overwrite existing data",
            required=False,
            default=False,
        ),
        th.Property(
            "table_configs",
            th.ArrayType(
                th.ObjectType(
                    th.Property(
                        "table_name",
                        th.StringType,
                        description="Name of the table (including prefix if that is configured)",  # noqa: E501
                        required=False,
                        default=None,
                    ),
                    th.Property(
                        "table_prefix",
                        th.StringType,
                        description="Prefix of the table (including prefix if that is configured)",  # noqa: E501
                        required=False,
                        default=None,
                    ),
                    th.Property(
                        "partition_column",
                        th.StringType,
                        description="Column to partition table by. Must be a timestamp",
                        required=False,
                        default=None,
                    ),
                    th.Property(
                        "truncate_before_load",
                        th.BooleanType,
                        description="If target table should be truncated before new data is loaded",  # noqa: E501
                        required=False,
                        default=None,
                    ),
                    th.Property(
                        "append_only",
                        th.BooleanType,
                        description="Only append to table; don't overwrite existing data",  # noqa: E501
                        required=False,
                        default=None,
                    ),
                )
            ),
            description="Stream specific configs. Like partition keys.",
            required=False,
            default=None,
        ),
    ).to_dict()

    default_sink_class = BigQuerySink

    @property
    def _MAX_RECORD_AGE_IN_MINUTES(self) -> float:  # type: ignore
        return float(self.config.get("max_batch_age", 5.0))

    def _process_batch_message(self, message_dict: dict) -> None:
        """Overridden because Meltano 0.11.1 has a bad implementation see
        https://github.com/meltano/sdk/issues/1031
        """
        stream_name = message_dict["stream"]
        for stream_map in self.mapper.stream_maps[stream_name]:
            sink = self.get_sink(stream_map.stream_alias)

            encoding = BaseBatchFileEncoding.from_dict(message_dict["encoding"])
            sink.process_batch_files(
                encoding,
                message_dict["manifest"],
            )
            # Respect if a batch tap sends state messages
            self.batch_msg_processed = True

    def _process_activate_version_message(self, message_dict: dict) -> None:
        # Bug in meltano sdk with stream maps:
        # https://github.com/meltano/sdk/issues/1055
        pass

    def _process_schema_message(self, message_dict: dict) -> None:
        """Process a SCHEMA messages.

        Args:
            message_dict: The newly received schema message.
        """
        self.logger.info(
            f"Received schema for {message_dict['stream']}: {message_dict['schema']}"
        )
        super()._process_schema_message(message_dict)

    def _process_state_message(self, message_dict: dict) -> None:
        """Process a state message. drain sinks if needed.

        If state is unchanged, no actions will be taken.

        Args:
            message_dict: TODO
        """
        self._assert_line_requires(message_dict, requires={"value"})
        state = message_dict["value"]
        if self._latest_state == state:
            return
        self._latest_state = state
        if (
            self.batch_msg_processed
            or self._max_record_age_in_minutes > self._MAX_RECORD_AGE_IN_MINUTES
        ):
            # This will drain all stored records and emit state
            self.drain_all()


if __name__ == "__main__":
    TargetBigQuery.cli()
