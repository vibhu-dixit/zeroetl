import json
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from pyspark.sql import Row

from zeroetl import ingestion, table_management


def test_result_serializes_nested_spark_values():
    from zeroetl.results import OperationResult

    result = OperationResult(
        "query", "mycatalog.db.users",
        counts={"returned": 1},
        rows=[Row(id=1, nested=Row(day=date(2024, 1, 2), amount=Decimal("1.20")),
                  times=[datetime(2024, 1, 2, 3, 4, 5)])],
    )
    assert json.loads(json.dumps(result.to_dict())) == {
        "operation": "query", "table_name": "mycatalog.db.users", "status": "success",
        "counts": {"returned": 1},
        "rows": [{"id": 1, "nested": {"day": "2024-01-02", "amount": "1.20"},
                  "times": ["2024-01-02T03:04:05"]}],
        "message": None,
    }


@pytest.mark.parametrize("operation", [
    table_management.create_iceberg_table,
    table_management.compact_iceberg_table,
    table_management.expire_iceberg_snapshots,
    table_management.query_iceberg_table,
])
def test_table_operation_returns_result_and_preserves_caller_session(operation):
    from zeroetl.results import OperationResult

    spark = MagicMock()
    spark.sql.return_value.limit.return_value.collect.return_value = [Row(id="1")]
    result = operation({"table_name": "mycatalog.db.users"}, spark=spark)
    assert isinstance(result, OperationResult)
    assert result.table_name == "mycatalog.db.users"
    if operation is table_management.query_iceberg_table:
        assert result.rows == [{"id": "1"}]
    spark.stop.assert_not_called()


def test_ingestion_returns_result_and_preserves_caller_session(monkeypatch):
    from zeroetl.results import OperationResult
    from pyspark.sql.types import StringType, StructField, StructType, TimestampType

    spark = MagicMock()
    schema = StructType([StructField("id", StringType()), StructField("signup_ts", TimestampType())])
    df = MagicMock()
    df.schema = schema
    df.count.return_value = 0
    spark.read.schema.return_value.option.return_value.csv.return_value = df
    spark.table.return_value.schema = schema
    df.filter.return_value.count.return_value = 0
    class Predicate:
        def isNotNull(self):
            return self
        def __and__(self, other):
            return self
        def __invert__(self):
            return self
    monkeypatch.setattr(ingestion.F, "col", lambda _name: Predicate())
    result = ingestion.ingest_data_to_iceberg("input.csv", schema, "csv", {}, spark=spark)
    assert isinstance(result, OperationResult)
    assert result.operation == "ingest"
    assert result.counts == {"received": 0, "accepted": 0, "quarantined": 0}
    spark.stop.assert_not_called()


def test_shared_spark_builder_omits_missing_aws_credentials(monkeypatch):
    from zeroetl import spark as spark_module

    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.setattr(spark_module, "load_dotenv", lambda: None)
    builder = MagicMock()
    builder.appName.return_value = builder
    builder.config.return_value = builder
    monkeypatch.setattr(spark_module.SparkSession, "builder", builder)
    spark_module.get_spark_session("test", {"spark.sql.shuffle.partitions": "2"}, "file:///tmp/warehouse")
    options = [call.args for call in builder.config.call_args_list]
    assert ("spark.sql.catalog.mycatalog.warehouse", "file:///tmp/warehouse") in options
    assert ("spark.sql.shuffle.partitions", "2") in options
    assert not any(key in {"spark.hadoop.fs.s3a.access.key", "spark.hadoop.fs.s3a.secret.key"}
                   for key, _ in options)


def test_time_travel_rejects_nonpositive_or_noninteger_snapshot_ids():
    for snapshot_id in (0, -1, "1"):
        spark = MagicMock()
        with pytest.raises(ValueError, match="snapshot ID"):
            table_management.query_iceberg_table(
                {"query_type": "time_travel", "snapshot_id": snapshot_id}, spark=spark,
            )
        spark.sql.assert_not_called()
