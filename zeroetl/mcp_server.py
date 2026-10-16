"""Local stdio MCP access to the existing Iceberg operations."""

import base64
from contextlib import asynccontextmanager, redirect_stdout
from datetime import date, datetime
from decimal import Decimal
import math
import os
from pathlib import Path
import re
import sys
from typing import Literal, TypedDict
from urllib.parse import urlsplit

from pyspark.sql.types import (BooleanType, DateType, DoubleType, IntegerType,
                               LongType, StringType, StructField, StructType,
                               TimestampType)

from .ingestion import get_spark_session, ingest_data_to_iceberg
from .table_management import (compact_iceberg_table, create_iceberg_table,
                               expire_iceberg_snapshots, query_iceberg_table)

try:
    from mcp.server.mcpserver import MCPServer
    from mcp.types import ToolAnnotations
    from typing_extensions import TypedDict
except ImportError:
    MCPServer = None


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TABLE = re.compile(r"^mycatalog\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$")
_TYPES = {
    "STRING": StringType, "INT": IntegerType, "BIGINT": LongType,
    "DOUBLE": DoubleType, "BOOLEAN": BooleanType, "DATE": DateType,
    "TIMESTAMP": TimestampType,
}


class Column(TypedDict):
    name: str
    type: Literal["STRING", "INT", "BIGINT", "DOUBLE", "BOOLEAN", "DATE", "TIMESTAMP"]


def validate_table(table_name: str) -> str:
    if not isinstance(table_name, str) or not _TABLE.fullmatch(table_name):
        raise ValueError("table_name must be mycatalog.<namespace>.<table>")
    return table_name


def validate_schema(schema: list[Column]) -> list[Column]:
    if not isinstance(schema, list) or not schema:
        raise ValueError("schema must have at least one column")
    names = set()
    for column in schema:
        if (not isinstance(column, dict) or not isinstance(column.get("name"), str)
                or not _IDENTIFIER.fullmatch(column["name"]) or column.get("type") not in _TYPES):
            raise ValueError("invalid column name or type")
        if column["name"] in names:
            raise ValueError("duplicate column name")
        names.add(column["name"])
    return schema


def validate_partition(partition_spec: list[str], columns: set[str] | None = None) -> str:
    if not isinstance(partition_spec, list) or not partition_spec:
        raise ValueError("partition_spec must have at least one field")
    parts = []
    for part in partition_spec:
        if not isinstance(part, str):
            raise ValueError("invalid partition field")
        match = re.fullmatch(r"(years|months|days|hours)\(([A-Za-z_][A-Za-z0-9_]*)\)|bucket\(([1-9][0-9]{0,3}), ([A-Za-z_][A-Za-z0-9_]*)\)|([A-Za-z_][A-Za-z0-9_]*)", part)
        if not match:
            raise ValueError("invalid partition field")
        column = match.group(2) or match.group(4) or match.group(5)
        if columns is not None and column not in columns:
            raise ValueError("partition field must name a schema column")
        parts.append(part)
    return ", ".join(parts)


def validate_snapshot(snapshot_id: int) -> int:
    if type(snapshot_id) is not int or snapshot_id <= 0:
        raise ValueError("snapshot_id must be a positive integer")
    return snapshot_id


def validate_retention(seconds: int) -> int:
    if type(seconds) is not int or not 1 <= seconds <= 365 * 24 * 60 * 60:
        raise ValueError("older_than_seconds must be between 1 and 31536000")
    return seconds


def _safe_uri(value: str) -> bool:
    uri = urlsplit(value)
    # shortcut: encoded URI paths are unsupported until canonical decoding is defined.
    return (uri.scheme in {"s3", "s3a", "file"} and bool(uri.netloc or uri.scheme == "file")
            and not uri.query and not uri.fragment and "%" not in uri.path
            and ".." not in uri.path.split("/"))


def validate_location(location: str) -> str:
    if not isinstance(location, str) or not location or "'" in location or "\n" in location or "\r" in location:
        raise ValueError("invalid location")
    if "://" in location:
        if not _safe_uri(location):
            raise ValueError("invalid location URI")
    elif not Path(location).is_absolute() or ".." in Path(location).parts:
        raise ValueError("location must be an absolute path or supported URI")
    return location


def validate_path(path: str) -> str:
    if isinstance(path, str) and any(char in path for char in "*?[]{}"):
        raise ValueError("CSV input path must not contain Spark glob characters")
    prefixes = [value.strip() for value in os.getenv("ZEROETL_MCP_ALLOWED_INPUT_PREFIXES", "").split(",") if value.strip()]
    if not isinstance(path, str) or not path or not prefixes:
        raise ValueError("CSV input requires ZEROETL_MCP_ALLOWED_INPUT_PREFIXES")
    if "://" in path:
        if not _safe_uri(path):
            raise ValueError("invalid CSV URI")
        allowed = any(path == prefix.rstrip("/") or path.startswith(prefix.rstrip("/") + "/") for prefix in prefixes if "://" in prefix and _safe_uri(prefix))
    else:
        candidate = Path(path).resolve()
        allowed = any(candidate.is_relative_to(Path(prefix).resolve()) for prefix in prefixes if "://" not in prefix and Path(prefix).is_absolute())
    if not allowed:
        raise ValueError("CSV input is outside allowed prefixes")
    return path


def _json_safe(value):
    if hasattr(value, "asDict"):
        value = value.asDict(recursive=True)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, float) and not math.isfinite(value):
        return str(value).replace("nan", "NaN").replace("inf", "Infinity")
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"unsupported query value: {type(value).__name__}")


def create_server():
    if MCPServer is None:
        raise RuntimeError("Install the optional MCP SDK with: pip install zeroetl[mcp]")

    state = {}

    @asynccontextmanager
    async def lifespan(_server):
        with redirect_stdout(sys.stderr):
            state["spark"] = get_spark_session(app_name="ZeroETLMCP")
        try:
            yield state
        finally:
            state.pop("spark").stop()

    server = MCPServer("zeroetl", lifespan=lifespan, log_level="WARNING")

    @server.tool(annotations=ToolAnnotations(readOnlyHint=True))
    def query_table(table_name: str, query_type: Literal["latest", "time_travel"] = "latest", snapshot_id: int | None = None) -> list[dict]:
        table_name = validate_table(table_name)
        if query_type not in ("latest", "time_travel") or (query_type == "time_travel" and snapshot_id is None):
            raise ValueError("time_travel requires snapshot_id")
        if snapshot_id is not None:
            validate_snapshot(snapshot_id)
        with redirect_stdout(sys.stderr):
            result = query_iceberg_table({"table_name": table_name, "query_type": query_type, "snapshot_id": snapshot_id}, spark=state["spark"])
        return result.to_dict()["rows"]

    if os.getenv("ZEROETL_MCP_ENABLE_WRITES") == "true":
        @server.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=True))
        def ingest_csv(table_name: str, path: str, schema: list[Column], primary_key_col: str, timestamp_col: str) -> dict:
            table_name = validate_table(table_name)
            path = validate_path(path)
            schema = validate_schema(schema)
            columns = {field["name"] for field in schema}
            if primary_key_col not in columns or timestamp_col not in columns:
                raise ValueError("primary key and timestamp columns must be in schema")
            spark_schema = StructType([StructField(field["name"], _TYPES[field["type"]](), True) for field in schema])
            with redirect_stdout(sys.stderr):
                ingest_data_to_iceberg(path, spark_schema, "csv", {"table_name": table_name, "primary_key_col": primary_key_col, "timestamp_col": timestamp_col}, spark=state["spark"])
            return {"status": "ok"}

        @server.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False))
        def create_table(table_name: str, schema: list[Column], partition_spec: list[str], location: str) -> dict:
            table_name = validate_table(table_name)
            schema = validate_schema(schema)
            partition = validate_partition(partition_spec, {field["name"] for field in schema})
            location = validate_location(location)
            with redirect_stdout(sys.stderr):
                create_iceberg_table({"table_name": table_name, "schema": ", ".join(f'{field["name"]} {field["type"]}' for field in schema), "partition_spec": partition, "location": location}, spark=state["spark"])
            return {"status": "ok"}

        @server.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=True))
        def compact_table(table_name: str) -> dict:
            with redirect_stdout(sys.stderr):
                compact_iceberg_table({"table_name": validate_table(table_name)}, spark=state["spark"])
            return {"status": "ok"}

        @server.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=True))
        def expire_snapshots(table_name: str, older_than_seconds: int) -> dict:
            table_name = validate_table(table_name)
            older_than_seconds = validate_retention(older_than_seconds)
            with redirect_stdout(sys.stderr):
                expire_iceberg_snapshots({"table_name": table_name, "older_than_seconds": older_than_seconds}, spark=state["spark"])
            return {"status": "ok"}

    return server


def main():
    create_server().run(transport="stdio")


if __name__ == "__main__":
    main()
