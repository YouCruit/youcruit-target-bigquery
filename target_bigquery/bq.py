"""Some BigQuery utils"""
from typing import Iterable, Optional

from google.cloud.bigquery import Client, SchemaField


def get_client(project_id: str, location: Optional[str] = None) -> Client:
    """Returns a Google Client. This is a method so it can be mocked in tests."""
    return Client(project=project_id, location=location)


def column_type(name: str, schema_property: dict, nullable: bool) -> SchemaField:
    """Converts to big query type"""
    if "anyOf" in schema_property and len(schema_property["anyOf"]) > 0:
        return SchemaField(name, "string", "NULLABLE")

    property_type = schema_property["type"]

    property_format = schema_property.get("format", None)

    if "array" in property_type:
        try:
            items_schema = schema_property["items"]
            items_type = bigquery_type(
                items_schema["type"], items_schema.get("format", None)
            )
        except KeyError:
            return SchemaField(name, "string", "NULLABLE" if nullable else "REQUIRED")
        else:
            if items_type == "record":
                return handle_record_type(name, items_schema, "REPEATED")
            return SchemaField(name, items_type, "REPEATED")

    elif "object" in property_type:
        return handle_record_type(
            name, schema_property, mode="NULLABLE" if nullable else "REQUIRED"
        )

    else:
        result_type = bigquery_type(property_type, property_format)
        return SchemaField(name, result_type, "NULLABLE" if nullable else "REQUIRED")


def handle_record_type(name, schema_property, mode="NULLABLE") -> SchemaField:
    fields = [
        column_type(col, t, True)
        for col, t in schema_property.get("properties", {}).items()
    ]  # type: ignore
    if fields:
        return SchemaField(name, "RECORD", mode, fields=fields)
    else:
        return SchemaField(name, "string", mode)


def bigquery_type(property_type: Iterable, property_format: str) -> str:
    # Every date-time JSON value is currently mapped to TIMESTAMP WITHOUT TIME ZONE
    #
    # TODO: Detect if timezone postfix exists in the JSON and find if DATETIME or
    # TIMESTAMP which includes time zone is the better column type
    if property_format == "date-time":
        return "timestamp"
    elif property_format == "time":
        return "time"
    elif "number" in property_type:
        return "numeric"
    elif "integer" in property_type and "string" in property_type:
        return "string"
    elif "integer" in property_type:
        return "integer"
    elif "boolean" in property_type:
        return "boolean"
    elif "object" in property_type:
        return "record"
    else:
        return "string"
