"""BigQuery target sink class, which handles writing streams."""

from __future__ import annotations
from datetime import datetime, timedelta
from io import BufferedRandom
from pathlib import Path
from tempfile import mkstemp
from typing import Dict, List, Optional, Union

from singer_sdk.sinks import BatchSink
from singer_sdk.plugin_base import PluginBase
from fastavro import writer, parse_schema, reader
from google.cloud import bigquery

from .bq import column_type
from .avro import avro_schema, fix_recursive_types_in_dict

class BigQuerySink(BatchSink):
    """BigQuery target sink class."""

    # Max records to write in one batch
    max_size = 10000

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)

        self.project_id = target.config['project_id']
        self.location = target.config.get('location', None)
        self.dataset_id = target.config['dataset']
        self.table_prefix = target.config.get('table_prefix', None)

        self.parsed_schema = parse_schema(avro_schema(self.stream_name, self.schema))

        self.client: bigquery.Client = target.get_client(self.project_id, self.location)

    @property
    def table_name(self) -> str:
        """Get table name.

        Returns:
            Table name for this sink
        """
        return f"{self.table_prefix or ''}{self.stream_name}"

    def temp_table_name(self, batch_id: str) -> str:
        """Get temporary table name.

        Returns:
            Temporary table name for this sink and batch
        """
        return f"{self.table_name}_{batch_id}"

    def create_table(self, table_name: str, expires: bool) -> str:
        """Creates the table in bigquery"""

        self.logger.info(f"{self.schema}")

        schema = [
            column_type(name, schema)
            for (name, schema) in self.schema['properties'].items()
        ]

        table = bigquery.Table(
            f"{self.project_id}.{self.dataset_id}.{table_name}",
            schema
        )

        if expires:
            table.expires = datetime.now() + timedelta(days=1)

        self.client.create_table(
            table=table,
            exists_ok=True
        )

    def update_from_temp_table(self, batch_id: str) -> str:
        """Returns a MERGE query"""
        primary_keys = ' AND '.join([
            f"src.`{k}` = dst.`{k}`"
            for k in self.key_properties
        ])
        set_values = ', '.join([
            f"`{key}` = src.`{key}`"
            for key in self.schema["properties"].keys()
        ])
        cols = ', '.join([
            f"`{key}`"
            for key in self.schema["properties"].keys()
        ])

        return f"""MERGE `{self.dataset_id}`.`{self.table_name}` dst
            USING `{self.dataset_id}`.`{self.temp_table_name(batch_id)}` src
            ON {primary_keys}
            WHEN MATCHED THEN
                UPDATE SET {set_values}
            WHEN NOT MATCHED THEN
                INSERT ({cols}) VALUES ({cols})"""

    def drop_temp_table(self, batch_id: str) -> str:
        """Returns a DROP TABLE statement"""
        return f"DROP TABLE `{self.dataset_id}`.`{self.temp_table_name(batch_id)}`"

    def table_exists(self) -> bool:
        """Check if table already exists"""
        tables = self.query([f"SELECT table_name FROM {self.dataset_id}.INFORMATION_SCHEMA.TABLES"])
        for table in tables:
            table_name = table['table_name']
            self.logger.info(f"FOund table: {table_name}")

            if table_name == self.table_name:
                return True
        return False

    def query(self, queries: List[str]) -> bigquery.RowIterator:
        """Executes the queries and returns when they have completed"""
        job_config = bigquery.QueryJobConfig()
        job_config.query_parameters = []

        joined_queries = ';\n'.join(queries)

        self.logger.info(joined_queries)

        query_job = self.client.query(joined_queries, job_config=job_config)

        # Starts job and waits for result
        return query_job.result()

    def load_table_from_file(self, filehandle: BufferedRandom, batch_id: str) -> None:
        """Starts a load job for the given file. Returns once the job is completed"""
        self.create_table(self.temp_table_name(batch_id), expires=True)

        dataset_ref = self.client.dataset(self.dataset_id)
        table_ref = dataset_ref.table(self.temp_table_name(batch_id))

        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.AVRO
        job_config.use_avro_logical_types = True
        job_config.write_disposition = 'WRITE_TRUNCATE'

        job = self.client.load_table_from_file(filehandle, table_ref, job_config=job_config)

        # Starts job and waits for result
        job.result()
        self.logger.info("Load complete")


    def start_batch(self, context: dict) -> None:
        """Start a batch.

        Developers may optionally add additional markers to the `context` dict,
        which is unique to this batch.
        """
        # Sample:
        # ------
        # batch_key = context["batch_id"]
        # context["file_path"] = f"{batch_key}.csv"

        if not self.key_properties:
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
        # TODO make debug
        self.logger.info(f"Processing record in batch {context['batch_id']}")

        avro_record = fix_recursive_types_in_dict(record, self.schema['properties'])

        self.logger.info(f"Avro record: {avro_record}")

        if "records" not in context:
            context["records"] = []

        context["records"].append(avro_record)

        # Appending doesn't write schema
        #with open(context["temp_file"], "a+b") as tempfile:
        #    writer(tempfile, self.parsed_schema, [avro_record])

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written."""
        # Sample:
        # ------
        # client.upload(context["file_path"])  # Upload file
        # Path(context["file_path"]).unlink()  # Delete local copy
        self.logger.info(f"Processing batch {context['batch_id']}")

        with open(context["temp_file"], "wb") as tempfile:
            writer(tempfile, self.parsed_schema, context["records"])

        with open(context["temp_file"], "r+b") as tempfile:
            self.load_table_from_file(tempfile, context["batch_id"])

        # TODO Delete temp file once we are done with it
        self.logger.info(f"WOuld have deleted: {context['temp_file']}")
        with open(context["temp_file"], "r+b") as tempfile:
            for n in reader(tempfile):
                self.logger.info(f"Avro: {n}")
        #Path(context["temp_file"]).unlink()

        # Finally merge data and delete temp table
        if not self.table_exists():
            self.logger.info(f"{self.table_name} does not exist. Creating")

        self.create_table(self.table_name, expires=False)

        self.query([
            self.update_from_temp_table(context["batch_id"]),
            self.drop_temp_table(context["batch_id"]),
        ])


