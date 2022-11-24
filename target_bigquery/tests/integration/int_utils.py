"""Utility functions for integration tests"""
import os
from typing import Optional

from google.cloud import bigquery


def get_client() -> bigquery.Client:
    """Returns Google client"""
    return bigquery.Client(
        project=os.environ.get("TARGET_BIGQUERY_PROJECT_ID"),
        location=os.environ.get("TARGET_BIGQUERY_LOCATION"),
    )


def query(client: bigquery.Client, table_name: str, column_name: str) -> Optional[list]:
    """
    Queries big query and returns a list of dicts with column names and values.
    None if no such table.
    """
    # TODO
    return None
