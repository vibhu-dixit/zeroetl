from unittest.mock import MagicMock

import pytest
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from zeroetl import ingestion, table_management


@pytest.mark.parametrize(
    "operation",
    [
        table_management.create_iceberg_table,
        table_management.query_iceberg_table,
        table_management.compact_iceberg_table,
        table_management.expire_iceberg_snapshots,
    ],
)
def test_caller_owned_session_survives_operation_error(operation):
    spark = MagicMock()
    spark.sql.side_effect = RuntimeError("Spark failed")
    with pytest.raises(RuntimeError, match="Spark failed"):
        operation({"table_name": "mycatalog.db.users"}, spark=spark)
    spark.stop.assert_not_called()


def test_owned_session_stops_when_table_creation_fails(monkeypatch):
    spark = MagicMock()
    spark.sql.side_effect = RuntimeError("Spark failed")
    monkeypatch.setattr(table_management, "get_spark_session", lambda **_: spark)
    with pytest.raises(RuntimeError, match="Spark failed"):
        table_management.create_iceberg_table({})
    spark.stop.assert_called_once()


def test_caller_owned_ingestion_session_survives_reader_error():
    spark = MagicMock()
    spark.read.schema.side_effect = RuntimeError("CSV failed")
    with pytest.raises(RuntimeError, match="CSV failed"):
        ingestion.ingest_data_to_iceberg(
            "input.csv", StructType([StructField("id", StringType()), StructField("signup_ts", TimestampType())]),
            "csv", {}, spark=spark,
        )
    spark.stop.assert_not_called()


def test_owned_ingestion_session_stops_when_reader_fails(monkeypatch):
    spark = MagicMock()
    spark.read.schema.side_effect = RuntimeError("CSV failed")
    monkeypatch.setattr(ingestion, "get_spark_session", lambda **_: spark)
    with pytest.raises(RuntimeError, match="CSV failed"):
        ingestion.ingest_data_to_iceberg(
            "input.csv", StructType([StructField("id", StringType()), StructField("signup_ts", TimestampType())]), "csv", {},
        )
    spark.stop.assert_called_once()
