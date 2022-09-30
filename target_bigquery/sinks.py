"""BigQuery target sink class, which handles writing streams."""

from __future__ import annotations
from pathlib import Path
from tempfile import mkstemp
from typing import Dict, List, Optional, Union

from singer_sdk.sinks import BatchSink
from singer_sdk.plugin_base import PluginBase
from fastavro import writer, parse_schema
from google.cloud import bigquery
from google.cloud.bigquery.table import RowIterator

from target_bigquery.avro import avro_schema, fix_recursive_types_in_dict

class BigQuerySink(BatchSink):
    """BigQuery target sink class."""

    max_size = 10000  # Max records to write in one batch

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)

        project_id = target.config['project_id']
        location = target.config.get('location', None)
        self.dataset_id = target.config['dataset']

        self.parsed_schema = parse_schema(avro_schema(project_id, self.dataset_id, self.schema))

        self.client = bigquery.Client(project=project_id, location=location)


    @property
    def table_name(self) -> str:
        """Get table name.

        Returns:
            Table name for this sink
        """
        # TODO tap prefix??
        return self.schema["stream"]

    @property
    def temp_table_name(self) -> str:
        """Get temporary table name.

        Returns:
            Temporary table name for this sink
        """

        return f"{self.table_name}_temp"

    @property
    def update_from_temp_table(self) -> str:
        """Returns a MERGE query"""
        primary_keys = ' AND '.join([
            f"src.`{k}` = dst.`{k}`"
            for k in self.schema["key_properties"]
        ])
        set_values = ', '.join([
            f"`{key}` = src.`{key}`"
            for key in self.schema["schema"]["properties"].keys()
        ])
        cols = ', '.join([
            f"`{key}`"
            for key in self.schema["schema"]["properties"].keys()
        ])

        return f"""MERGE `{self.dataset_id}`.`{self.table_name}` dst
            USING `{self.dataset_id}`.`{self.temp_table_name}` src
            ON {primary_keys}
            WHEN MATCHED THEN
                UPDATE SET {set_values}
            WHEN NOT MATCHED THEN
                INSERT ({cols}) VALUES ({cols})"""

    @property
    def drop_temp_table(self) -> str:
        """Returns a DROP TABLE statement"""
        return f"DROP TABLE {self.dataset_id}.{self.table_name}"

    def query(self, queries: List[str]) -> None:
        """Executes the queries and returns when they have completed"""
        job_config = bigquery.QueryJobConfig()
        job_config.query_parameters = []

        query_job = self.client.query(';\n'.join(queries), job_config=job_config)

        # Starts job and waits for result
        query_job.result()

    def load_table_from_file(self, filehandle) -> None:
        """Starts a load job for the given file. Returns once the job is completed"""
        dataset_ref = self.client.dataset(self.dataset_id)
        table_ref = dataset_ref.table(self.temp_table_name)

        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.AVRO
        job_config.use_avro_logical_types = True
        job_config.write_disposition = 'WRITE_TRUNCATE'

        job = self.client.load_table_from_file(filehandle, table_ref, job_config=job_config)

        # Starts job and waits for result
        job.result()


    def start_batch(self, context: dict) -> None:
        """Start a batch.

        Developers may optionally add additional markers to the `context` dict,
        which is unique to this batch.
        """
        # Sample:
        # ------
        # batch_key = context["batch_id"]
        # context["file_path"] = f"{batch_key}.csv"
        self.logger.info(f"Starting batch with {self.current_size} records")

        if not self.schema["key_properties"]:
            raise Exception("Missing key_properties (e.g. primary key(s)) in schema!")

        _, temp_file = mkstemp()
        context["temp_file"] = temp_file

    def process_record(self, record: dict, context: dict) -> None:
        """Process the record.

        Developers may optionally read or write additional markers within the
        passed `context` dict from the current batch.
        """
        # Sample:
        # ------
        # with open(context["file_path"], "a") as csvfile:
        #     csvfile.write(record)

        avro_record = fix_recursive_types_in_dict(record, self.schema)

        with open(context["temp_file"], "ab") as tempfile:
            writer(tempfile, self.parsed_schema, [avro_record])

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written."""
        # Sample:
        # ------
        # client.upload(context["file_path"])  # Upload file
        # Path(context["file_path"]).unlink()  # Delete local copy

        with open(context["temp_file"], "r+b") as tempfile:
            self.load_table_from_file(tempfile)

        # Delete temp file once we are done with it
        Path(context["temp_file"]).unlink()

        # Finally merge data and delete temp table
        self.query([
            self.update_from_temp_table,
            self.drop_temp_table,
        ])
        self.logger.info(f"Finished batch with {self.current_size} records")


