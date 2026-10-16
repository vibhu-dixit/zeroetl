from unittest.mock import MagicMock

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

from zeroetl.ingestion import ingest_data_to_iceberg


@pytest.fixture(autouse=True)
def spark_expressions(monkeypatch):
    from zeroetl import ingestion

    class Expression:
        def isNotNull(self): return self
        def isNull(self): return self
        def __and__(self, other): return self
        def __invert__(self): return self
        def over(self, window): return self
        def otherwise(self, value): return self
        def cast(self, data_type): return self

    for name in ("col", "lit", "when", "desc", "xxhash64", "row_number"):
        monkeypatch.setattr(ingestion.F, name, lambda *args, **kwargs: Expression())
    class WindowSpec:
        def orderBy(self, *expressions): return self
    monkeypatch.setattr(ingestion.Window, "partitionBy", lambda *columns: WindowSpec())


def make_call(config=None, *, input_schema=None, target_schema=None, received=2, invalid_count=0):
    input_schema = input_schema or StructType([
        StructField("id", StringType(), False),
        StructField("signup_ts", TimestampType(), False),
    ])
    target_schema = target_schema or input_schema
    spark = MagicMock()
    df = MagicMock()
    invalid = MagicMock()
    df.schema = input_schema
    df.count.return_value = received
    invalid.count.return_value = invalid_count
    df.filter.side_effect = [invalid, df]
    df.select.return_value = df
    df.withColumn.return_value = df
    reader = MagicMock()
    reader.option.return_value = reader
    reader.csv.return_value = df
    spark.read.schema.return_value = reader
    spark.table.return_value.schema = target_schema
    return spark, df, invalid, config or {}


def call(spark, schema, config=None):
    return ingest_data_to_iceberg(
        "input.csv", schema, "csv", {"timestamp_col": "signup_ts", **(config or {})}, spark=spark,
    )


@pytest.mark.parametrize("mode", ["append", "upsert"])
def test_empty_input_is_noop_for_append_and_upsert(mode):
    spark, _, _, _ = make_call(received=0)
    schema = StructType([StructField("id", StringType(), False), StructField("signup_ts", TimestampType(), False)])
    result = call(spark, schema, {"mode": mode})
    assert result.counts == {"received": 0, "accepted": 0, "quarantined": 0}
    spark.sql.assert_not_called()


def test_empty_overwrite_clears_table_only_when_explicitly_selected():
    spark, df, _, _ = make_call(received=0)
    schema = StructType([StructField("id", StringType(), False), StructField("signup_ts", TimestampType(), False)])
    call(spark, schema, {"mode": "overwrite"})
    df.writeTo.return_value.overwrite.assert_called_once()


def test_append_keeps_duplicates_and_uses_insert_into():
    spark, df, _, _ = make_call(received=2)
    result = call(spark, df.schema, {"mode": "append"})
    df.writeTo.assert_called_once_with("mycatalog.db.users")
    df.writeTo.return_value.append.assert_called_once_with()
    assert result.counts == {"received": 2, "accepted": 2, "quarantined": 0}


def test_overwrite_replaces_table_data():
    spark, df, _, _ = make_call(received=2)
    call(spark, df.schema, {"mode": "overwrite"})
    df.writeTo.assert_called_once_with("mycatalog.db.users")
    df.writeTo.return_value.overwrite.assert_called_once()


def test_upsert_uses_merge_and_rejects_invalid_rows_before_writing():
    spark, df, _, _ = make_call(received=1, invalid_count=1)
    with pytest.raises(ValueError, match="invalid row"):
        call(spark, df.schema)
    spark.sql.assert_not_called()


def test_upsert_merges_accepted_rows():
    spark, df, _, _ = make_call(received=2)
    invalid = MagicMock()
    invalid.count.return_value = 0
    df.filter.side_effect = [invalid, df, df]
    result = call(spark, df.schema)
    assert "MERGE INTO mycatalog.db.users" in spark.sql.call_args.args[0]
    assert result.counts == {"received": 2, "accepted": 2, "quarantined": 0}


def test_quarantine_writes_invalid_rows_and_reports_counts():
    spark, df, invalid, _ = make_call(received=2, invalid_count=1)
    result = call(spark, df.schema, {"mode": "append", "invalid_rows": "quarantine", "quarantine_path": "bad/"})
    invalid.withColumn.return_value.write.mode.assert_called_once_with("append")
    invalid.withColumn.return_value.write.mode.return_value.parquet.assert_called_once_with("bad/")
    assert result.counts == {"received": 2, "accepted": 1, "quarantined": 1}


def test_quarantine_requires_a_path():
    schema = StructType([StructField("id", StringType()), StructField("ts", TimestampType())])
    with pytest.raises(ValueError, match="quarantine_path"):
        ingest_data_to_iceberg("input.csv", schema, "csv", {"invalid_rows": "quarantine"}, spark=MagicMock())


def test_schema_evolution_adds_nullable_columns_only():
    target = StructType([StructField("id", StringType(), False), StructField("signup_ts", TimestampType(), False)])
    incoming = StructType(target.fields + [StructField("new_col", IntegerType(), True)])
    spark, df, _, _ = make_call(input_schema=incoming, target_schema=target, received=1)
    call(spark, incoming, {"mode": "append", "evolve_schema": True})
    sql = " ".join(call.args[0] for call in spark.sql.call_args_list)
    assert "ADD COLUMN new_col int" in sql


def test_new_non_nullable_column_is_rejected():
    target = StructType([StructField("id", StringType(), False), StructField("signup_ts", TimestampType(), False)])
    incoming = StructType(target.fields + [StructField("new_col", IntegerType(), False)])
    spark, _, _, _ = make_call(input_schema=incoming, target_schema=target)
    with pytest.raises(ValueError, match="must be nullable"):
        call(spark, incoming, {"evolve_schema": True})
    spark.sql.assert_not_called()


def test_type_change_is_rejected():
    target = StructType([StructField("id", StringType()), StructField("signup_ts", TimestampType())])
    incoming = StructType([StructField("id", IntegerType()), StructField("signup_ts", TimestampType())])
    spark, _, _, _ = make_call(input_schema=incoming, target_schema=target)
    with pytest.raises(TypeError, match="Unsupported type change"):
        call(spark, incoming)
    spark.sql.assert_not_called()


def test_unknown_mode_is_rejected_before_spark_use():
    schema = StructType([StructField("id", StringType()), StructField("ts", TimestampType())])
    with pytest.raises(ValueError, match="mode must"):
        call(MagicMock(), schema, {"mode": "merge"})
