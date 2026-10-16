"""Command line access to ZeroETL."""

import argparse
import contextlib
import json
import re
import sys
from pathlib import Path


COMMANDS = ("check", "create-table", "ingest", "query", "compact", "expire-snapshots")
SPARK_TYPES = {
    "string": "StringType", "int": "IntegerType", "integer": "IntegerType",
    "bigint": "LongType", "long": "LongType", "float": "FloatType",
    "double": "DoubleType", "boolean": "BooleanType", "date": "DateType",
    "timestamp": "TimestampType", "binary": "BinaryType",
}


def _required(section, keys, name):
    if not isinstance(section, dict):
        raise ValueError(f"{name} must be a JSON object")
    for key in keys:
        if not isinstance(section.get(key), str) or not section[key].strip():
            raise ValueError(f"{name}.{key} must be a nonempty string")


def _validate(config, command):
    if not isinstance(config, dict):
        raise ValueError("config must be a JSON object")
    spark = config.get("spark")
    if not isinstance(spark, dict):
        raise ValueError("spark must be a JSON object")
    if "warehouse_path" in spark and (not isinstance(spark["warehouse_path"], str) or not spark["warehouse_path"].strip()):
        raise ValueError("spark.warehouse_path must be a nonempty string")
    if "spark_configs" in spark and not isinstance(spark["spark_configs"], dict):
        raise ValueError("spark.spark_configs must be a JSON object")
    if "spark_configs" in spark and not all(isinstance(key, str) for key in spark["spark_configs"]):
        raise ValueError("spark.spark_configs keys must be strings")
    table = config.get("table")
    _required(table, ["table_name"] if command != "create-table" else ["table_name", "schema_sql", "partition_spec"], "table")
    if "location" in table and table["location"] is not None and (not isinstance(table["location"], str) or not table["location"].strip()):
        raise ValueError("table.location must be a nonempty string or null")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}", table["table_name"]):
        raise ValueError("table.table_name must be a one-, two-, or three-part SQL identifier")
    if command == "ingest":
        ingestion = config.get("ingestion")
        if not isinstance(ingestion, dict):
            raise ValueError("ingestion must be a JSON object")
        if ingestion.get("source_type") not in ("csv", "pandas_df"):
            raise ValueError("ingestion.source_type must be csv or pandas_df")
        source = ingestion.get("input_data")
        if ingestion["source_type"] == "csv" and (not isinstance(source, str) or not source.strip()):
            raise ValueError("ingestion.input_data must be a CSV path")
        if ingestion["source_type"] == "pandas_df" and not isinstance(source, list):
            raise ValueError("ingestion.input_data must be a JSON array for pandas_df")
        fields = ingestion.get("schema")
        if not isinstance(fields, list) or not fields:
            raise ValueError("ingestion.schema must be a nonempty array")
        names = set()
        for field in fields:
            _required(field, ["name", "type"], "ingestion.schema field")
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", field["name"]):
                raise ValueError(f"invalid schema field name: {field['name']}")
            if field["name"] in names:
                raise ValueError(f"duplicate schema field: {field['name']}")
            names.add(field["name"])
            if field["type"].lower() not in SPARK_TYPES:
                raise ValueError(f"unknown Spark type: {field['type']}")
            if "nullable" in field and not isinstance(field["nullable"], bool):
                raise ValueError(f"nullable must be boolean for {field['name']}")
        if ingestion.get("mode", "upsert") not in ("upsert", "append", "overwrite"):
            raise ValueError("ingestion.mode must be upsert, append, or overwrite")
        if ingestion.get("invalid_rows", "fail") not in ("fail", "quarantine"):
            raise ValueError("ingestion.invalid_rows must be fail or quarantine")
        if ingestion.get("invalid_rows") == "quarantine":
            _required(ingestion, ["quarantine_path"], "ingestion")
        for key, default in (("primary_key_col", "id"), ("timestamp_col", "signup_ts")):
            value = ingestion.get(key, default)
            if not isinstance(value, str) or value not in names:
                raise ValueError(f"ingestion.{key} must name a schema field")
        if "evolve_schema" in ingestion and not isinstance(ingestion["evolve_schema"], bool):
            raise ValueError("ingestion.evolve_schema must be a boolean")
        if "timestamp_format" in ingestion and ingestion["timestamp_format"] is not None and not isinstance(ingestion["timestamp_format"], str):
            raise ValueError("ingestion.timestamp_format must be a string or null")
        if "csv_options" in ingestion and not isinstance(ingestion["csv_options"], dict):
            raise ValueError("ingestion.csv_options must be a JSON object")
    if command == "query":
        query_type = table.get("query_type", "latest")
        if query_type not in ("latest", "time_travel"):
            raise ValueError("table.query_type must be latest or time_travel")
        if query_type == "time_travel" and (type(table.get("snapshot_id")) is not int or table["snapshot_id"] <= 0):
            raise ValueError("table.snapshot_id must be a positive integer for time_travel")
    if command == "expire-snapshots" and "older_than_seconds" in table:
        if type(table["older_than_seconds"]) is not int or table["older_than_seconds"] < 0:
            raise ValueError("table.older_than_seconds must be a nonnegative integer")
    return config


def _redact(value):
    if isinstance(value, dict):
        redacted = {}
        for key, item in value.items():
            lowered = key.lower()
            if re.search(r"secret|password|token|credential|access[_-]?key", lowered):
                redacted[key] = "<redacted>"
            elif key == "input_data" and isinstance(item, list):
                redacted[key] = f"<inline records: {len(item)}>"
            else:
                redacted[key] = _redact(item)
        return redacted
    if isinstance(value, list):
        return [_redact(item) for item in value]
    return value


def _run(command, config):
    from .spark import get_spark_session

    spark_config = config["spark"]
    table = dict(config["table"])
    if "schema_sql" in table:
        table["schema"] = table.pop("schema_sql")
    table["warehouse_path"] = spark_config.get("warehouse_path")
    table["spark_configs"] = spark_config.get("spark_configs", {})
    spark = get_spark_session(app_name="ZeroETLCLI", spark_configs=spark_config.get("spark_configs"), warehouse_path=spark_config.get("warehouse_path"))
    try:
        if command == "check":
            from .results import OperationResult
            spark.sql("SELECT 1").collect()
            spark.sql("SHOW NAMESPACES IN mycatalog").collect()
            return OperationResult(operation="check", table_name=table["table_name"], message="Spark and Iceberg catalog are available")
        if command == "ingest":
            from .ingestion import ingest_data_to_iceberg
            from pyspark.sql import types

            ingestion = config["ingestion"]
            schema = types.StructType([
                types.StructField(field["name"], getattr(types, SPARK_TYPES[field["type"].lower()])(), field.get("nullable", True))
                for field in ingestion["schema"]
            ])
            source = ingestion["input_data"]
            if ingestion["source_type"] == "pandas_df":
                import pandas as pd
                source = pd.DataFrame(source)
            return ingest_data_to_iceberg(source, schema, ingestion["source_type"], {**table, **{k: v for k, v in ingestion.items() if k not in ("input_data", "schema", "source_type")}}, spark=spark)
        from . import table_management
        operation = getattr(table_management, {
            "create-table": "create_iceberg_table",
            "query": "query_iceberg_table",
            "compact": "compact_iceberg_table",
            "expire-snapshots": "expire_iceberg_snapshots",
        }[command])
        return operation(table, spark=spark)
    finally:
        spark.stop()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run ZeroETL operations from a JSON config")
    parser.add_argument("--config", required=True, help="JSON config file")
    parser.add_argument("--output", choices=("text", "json"))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("command", choices=COMMANDS)
    args = parser.parse_args(argv)
    output = args.output or "text"
    try:
        config = json.loads(Path(args.config).read_text(encoding="utf-8"))
        if args.output is None and isinstance(config, dict):
            output = config.get("output", output)
        if output not in ("text", "json"):
            raise ValueError("output must be text or json")
        _validate(config, args.command)
        if args.dry_run or config.get("dry_run", False):
            result = {"operation": args.command, "table_name": config["table"]["table_name"], "status": "dry_run", "plan": _redact(config)}
        else:
            with contextlib.redirect_stdout(sys.stderr):
                result = _run(args.command, config).to_dict()
        print(json.dumps(result) if output == "json" else json.dumps(result, indent=2))
        return 0
    except Exception as exc:
        error = {"operation": args.command, "status": "error", "message": str(exc)}
        print(json.dumps(error) if output == "json" else f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
