"""AVRO utility functions."""


import json
from datetime import date, datetime
from decimal import Decimal, getcontext
from typing import Any, Iterable, List, Optional, Union

PRECISION = 38
SCALE = 9
getcontext().prec = PRECISION
# Limit decimals to the same precision and scale as BigQuery accepts
ALLOWED_DECIMALS = Decimal(10) ** Decimal(-SCALE)
MAX_NUM = (Decimal(10) ** Decimal(PRECISION - SCALE)) - ALLOWED_DECIMALS


global schema_collision_counter
schema_collision_counter = 0


def fix_recursive_types_in_array(
    data: Optional[Iterable], props: dict
) -> Optional[list]:
    """
    Recursively walks the array to find datetimes and such
    """
    if data is None:
        return None

    return [
        fix_recursive_inner(datum, props) if datum is not None else None
        for datum in data
    ]


def fix_recursive_types_in_dict(data: Optional[dict], schema: dict) -> Optional[dict]:
    """
    Recursively walks the object to find datetimes and such
    """
    if data is None:
        return None

    result = {}
    for name, props in schema.items():
        if name in data and data[name] is not None:
            result[name] = fix_recursive_inner(data[name], props)
        else:
            result[name] = None
    return result


def fix_recursive_inner(
    value: Optional[Any], props: dict
) -> Optional[Union[str, Decimal, date, datetime, list, dict]]:
    """
    Recursively walks the item to find datetimes and such
    """
    if value is None:
        return None

    if "anyOf" in props:
        return json.dumps(value)
    elif is_unstructured_object(props):
        return json.dumps(value)
    # dump to string if array without items or recursive
    elif "array" in props["type"] and (
        "items" not in props or "$ref" in props["items"] or "type" not in props["items"]
    ):
        return json.dumps(value)
    elif "date-time" == props.get("format", "") and "string" in props.get("type", []):
        return parse_datetime(value)
    elif "number" in props.get("type", []):
        n = Decimal(value)
        return (
            MAX_NUM
            if n > MAX_NUM
            else -MAX_NUM
            if n < -MAX_NUM
            else n.quantize(ALLOWED_DECIMALS)
        )
    elif "object" in props.get("type", "") and "properties" in props:
        return fix_recursive_types_in_dict(value, props["properties"])
    elif "array" in props.get("type", []) and "items" in props:
        return fix_recursive_types_in_array(value, props["items"])
    else:
        return value


def is_unstructured_object(props: dict) -> bool:
    """Check if property is object and it has no properties."""
    return "object" in props["type"] and "properties" not in props


def parse_datetime(dt: Any) -> Optional[Union[datetime, date]]:
    try:
        if isinstance(dt, date):
            return dt

        if "Z" in dt:
            if "." in dt:
                # Seems like there are fractional seconds
                return datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S.%fZ")
            else:
                # Seems to be no fractional seconds
                return datetime.strptime(dt, "%Y-%m-%dT%H:%M:%SZ")

        return datetime.fromisoformat("+".join(dt.split("+")[:1]))
    except TypeError:
        return None


def column_type_avro(  # noqa: C901
    name: str, schema_property: dict, nullable: bool
) -> dict:
    result: dict[str, Union[str, dict, list]] = {"name": name}
    result_type: Union[str, dict[str, Union[Any, str]]] = ""

    global schema_collision_counter
    try:
        if "anyOf" in schema_property and len(schema_property["anyOf"]) > 0:
            result["type"] = ["null", "string"] if nullable else ["string"]
            return result
        else:
            property_type = schema_property["type"]
    except KeyError:
        raise KeyError(f"Column [{name}] did not have a defined type in the schema")

    property_format = schema_property.get("format", None)

    if "array" in property_type:
        try:
            items_type = column_type_avro(
                name,
                schema_property["items"],
                nullable=True,
            )  # type: ignore
            result_type = {"type": "array", "items": items_type["type"]}
        except KeyError:
            result_type = "string"
    elif "object" in property_type:
        items_types = [
            column_type_avro(col, schema_property, nullable=True)  # type: ignore
            for col, schema_property in schema_property.get("properties", {}).items()
        ]

        if items_types:
            # Avro tries to be smart and reuse schemas or something,
            # this causes collisions when different schemas end up
            # having the same name. So ensure that doesn't happen.
            schema_collision_counter += 1
            result_type = {
                "type": "record",
                "name": f"{name}_{schema_collision_counter}_properties",
                "fields": items_types,
            }
        else:
            result_type = "string"

    elif property_format == "date-time":
        result_type = {"type": "long", "logicalType": "timestamp-millis"}
    elif property_format == "time":
        result_type = {"type": "int", "logicalType": "time-millis"}
    elif "string" in property_type:
        result_type = "string"
    elif "number" in property_type:
        result_type = {
            "type": "bytes",
            "logicalType": "decimal",
            "scale": SCALE,
            "precision": PRECISION,
        }
    elif "integer" in property_type and "string" in property_type:
        result_type = "string"
    elif "integer" in property_type:
        result_type = "long"
    elif "boolean" in property_type:
        result_type = "boolean"
    else:
        result_type = "string"

    result["type"] = ["null", result_type] if nullable else [result_type]
    return result


def avro_schema(
    stream_name: str, schema_properties: dict, primary_keys: List[str]
) -> dict:
    schema = {
        "type": "record",
        "namespace": "youcruit.avro",
        # For some reason hyphens are not liked here
        "name": stream_name.replace("-", "_"),
        "fields": [
            column_type_avro(name, json_type, nullable=name not in primary_keys)
            for name, json_type in schema_properties.items()
        ],
    }

    return schema
