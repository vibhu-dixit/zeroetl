import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType, StructField
import pandas as pd
import os
import shutil

from zeroetl.ingestion import ingest_data_to_iceberg
from zeroetl.table_management import create_iceberg_table

# Fixture for creating a Spark Session for testing
@pytest.fixture(scope="session")
def spark_session():
    """
    Fixture to create a Spark session for tests.
    Sets up a local Spark environment with Iceberg extensions and a local warehouse.
    """
    test_warehouse_path = "/tmp/iceberg_test_warehouse"
    os.makedirs(test_warehouse_path, exist_ok=True) 
    spark = (
        SparkSession.builder
        .appName("ZeroETLTest")
        .master("local[*]")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.mycatalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.mycatalog.type", "hadoop")
        .config("spark.sql.catalog.mycatalog.warehouse", test_warehouse_path)
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.12.0")
        .getOrCreate()
    )
    yield spark
    spark.stop()
    if os.path.exists(test_warehouse_path):
        shutil.rmtree(test_warehouse_path)


@pytest.fixture(autouse=True)
def create_test_table(spark_session):
    table_name = "mycatalog.db.test_users"
    create_table_config = {
        "table_name": table_name,
        "schema": "id STRING, name STRING, email STRING, signup_ts TIMESTAMP, age INT",
        "partition_spec": "days(signup_ts)",
        "location": None,
        "warehouse_path": "/tmp/iceberg_test_warehouse" 
    }
    spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")
    create_iceberg_table(create_table_config, spark=spark_session)
    initial_pandas_data = pd.DataFrame({
        'id': ['1', '2'],
        'name': ['OldName', 'ExistingUser'],
        'email': ['old@example.com', 'existing@example.com'],
        'signup_ts': ['2023-01-01T12:00:00.000Z', '2023-01-02T13:00:00.000Z'],
        'age': [20, 30]
    })
    initial_schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("signup_ts", StringType(), True), 
        StructField("age", IntegerType(), True)
    ])

    ingestion_config_for_setup = {
        "table_name": table_name,
        "primary_key_col": "id",
        "timestamp_col": "signup_ts",
        "warehouse_path": "/tmp/iceberg_test_warehouse",
        "spark_configs": {}
    }

    ingest_data_to_iceberg(
        input_data=initial_pandas_data,
        schema=initial_schema,
        source_type='pandas_df',
        config=ingestion_config_for_setup,
        spark=spark_session,
    )

    yield table_name # Provide the table name to the test function

    # Cleanup after test: drop the table to ensure isolation between tests
    spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_ingest_data_deduplication_and_new_records(spark_session, create_test_table):
    """
    Tests if ingest_data_to_iceberg correctly handles updates (deduplication based on
    primary key and timestamp) and insertion of completely new records.
    """
    table_name = create_test_table # Get the test table name from the fixture
    
    # Data to be ingested:
    # - Record with 'id': '1' is an UPDATE because its signup_ts ('2023-01-01T14:00:00.000Z')
    #   is newer than the existing record for 'id': '1' ('2023-01-01T12:00:00.000Z').
    # - Record with 'id': '3' is a NEW record.
    input_data = pd.DataFrame({
        'id': ['1', '3'],
        'name': ['NewNameForAlice', 'NewUserBob'],
        'email': ['new_alice@example.com', 'new_bob@example.com'],
        'signup_ts': ['2023-01-01T14:00:00.000Z', '2023-01-03T15:00:00.000Z'],
        'age': [22, 45]
    })

    # Schema for the input data (matches `user_defined_schema` from examples, StringType for timestamp)
    ingestion_schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("signup_ts", StringType(), True),
        StructField("age", IntegerType(), True)
    ])

    # Ingestion configuration for the test run
    ingestion_config = {
        "table_name": table_name,
        "primary_key_col": "id",
        "timestamp_col": "signup_ts",
        "warehouse_path": "/tmp/iceberg_test_warehouse", # Use local test warehouse
        "spark_configs": {}
    }

    # Perform the ingestion
    ingest_data_to_iceberg(
        input_data=input_data,
        schema=ingestion_schema,
        source_type='pandas_df',
        config=ingestion_config,
        spark=spark_session,
    )
    
    # Verify the results after ingestion
    # Collect data and sort by 'id' for consistent assertion order
    result_df = spark_session.table(table_name).sort("id").collect()
    
    # Expected rows:
    # - '1': Updated by the newer record from input_data.
    # - '2': Original record, unchanged.
    # - '3': New record added.
    assert len(result_df) == 3, f"Expected 3 rows after ingestion, but got {len(result_df)}"
    
    # Check that the row with id '1' is updated with the new data
    updated_row = next((row for row in result_df if row.id == '1'), None)
    assert updated_row is not None
    assert updated_row.name == 'NewNameForAlice'
    assert updated_row.age == 22
    assert updated_row.email == 'new_alice@example.com' # Verify all updated fields
    assert updated_row.signup_ts.isoformat(timespec='milliseconds') + 'Z' == '2023-01-01T14:00:00.000Z' # Verify timestamp

    # Check original row '2' is still present and unchanged
    original_row_2 = next((row for row in result_df if row.id == '2'), None)
    assert original_row_2 is not None
    assert original_row_2.name == 'ExistingUser'
    assert original_row_2.age == 30
    assert original_row_2.email == 'existing@example.com'
    assert original_row_2.signup_ts.isoformat(timespec='milliseconds') + 'Z' == '2023-01-02T13:00:00.000Z'
    
    # Check that the new user '3' has been added
    new_row_3 = next((row for row in result_df if row.id == '3'), None)
    assert new_row_3 is not None
    assert new_row_3.name == 'NewUserBob'
    assert new_row_3.age == 45
    assert new_row_3.email == 'new_bob@example.com'
    assert new_row_3.signup_ts.isoformat(timespec='milliseconds') + 'Z' == '2023-01-03T15:00:00.000Z'

    print("Test passed: Data ingested and deduplicated correctly, and new records added.")

def test_ingest_data_older_timestamp_no_update(spark_session, create_test_table):
    """
    Tests that records with older timestamps for existing primary keys do NOT update
    the existing record, demonstrating the "latest record wins" deduplication.
    """
    table_name = create_test_table

    # Data to be ingested:
    # - Record for 'id': '1' has an OLDER timestamp ('2023-01-01T10:00:00.000Z')
    #   than the existing record for 'id': '1' ('2023-01-01T12:00:00.000Z') in the test setup.
    input_data_older = pd.DataFrame({
        'id': ['1'],
        'name': ['OldUpdateName'],
        'email': ['old_update@example.com'],
        'signup_ts': ['2023-01-01T10:00:00.000Z'], # This timestamp is intentionally older
        'age': [19]
    })

    ingestion_schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("signup_ts", StringType(), True),
        StructField("age", IntegerType(), True)
    ])

    ingestion_config = {
        "table_name": table_name,
        "primary_key_col": "id",
        "timestamp_col": "signup_ts",
        "warehouse_path": "/tmp/iceberg_test_warehouse",
        "spark_configs": {}
    }

    ingest_data_to_iceberg(
        input_data=input_data_older,
        schema=ingestion_schema,
        source_type='pandas_df',
        config=ingestion_config,
        spark=spark_session,
    )

    result_df = spark_session.table(table_name).sort("id").collect()

    # Check that there are still 2 rows (no new records, and no update for '1')
    assert len(result_df) == 2, f"Expected 2 rows after ingestion, but got {len(result_df)}"

    # Check that the row with id '1' retains its original data (not updated by older timestamp)
    existing_row_1 = next((row for row in result_df if row.id == '1'), None)
    assert existing_row_1 is not None
    assert existing_row_1.name == 'OldName' # Should still be original name from setup
    assert existing_row_1.age == 20 # Should still be original age from setup
    assert existing_row_1.email == 'old@example.com'
    assert existing_row_1.signup_ts.isoformat(timespec='milliseconds') + 'Z' == '2023-01-01T12:00:00.000Z' # Original timestamp

    print("Test passed: Records with older timestamps did not update existing data.")

def test_ingest_data_csv_source(spark_session, create_test_table, tmp_path):
    """
    Tests if ingest_data_to_iceberg correctly ingests data from a CSV file.
    """
    table_name = create_test_table
    
    # Create a temporary CSV file for the test
    csv_content = (
        "id,name,email,signup_ts,age\n"
        "1,UpdatedCsvName,updated_csv@example.com,2023-01-01T15:00:00.000Z,23\n"
        "4,NewUserFromCsv,csv_user@example.com,2023-01-04T10:00:00.000Z,28\n"
    )
    csv_file = tmp_path / "test_users.csv"
    csv_file.write_text(csv_content)

    ingestion_schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("signup_ts", StringType(), True),
        StructField("age", IntegerType(), True)
    ])

    ingestion_config = {
        "table_name": table_name,
        "primary_key_col": "id",
        "timestamp_col": "signup_ts",
        "warehouse_path": "/tmp/iceberg_test_warehouse",
        "spark_configs": {}
    }

    ingest_data_to_iceberg(
        input_data=str(csv_file), # Pass the path to the temporary CSV file
        schema=ingestion_schema,
        source_type='csv', # Specify 'csv' as source type
        config=ingestion_config,
        spark=spark_session,
    )

    result_df = spark_session.table(table_name).sort("id").collect()

    # Expected rows: '1' (updated by CSV), '2' (original), '4' (new from CSV)
    assert len(result_df) == 3, f"Expected 3 rows after CSV ingestion, but got {len(result_df)}"

    # Verify 'id': '1' updated from CSV
    updated_csv_row_1 = next((row for row in result_df if row.id == '1'), None)
    assert updated_csv_row_1 is not None
    assert updated_csv_row_1.name == 'UpdatedCsvName'
    assert updated_csv_row_1.age == 23

    # Verify 'id': '4' added from CSV
    new_csv_row_4 = next((row for row in result_df if row.id == '4'), None)
    assert new_csv_row_4 is not None
    assert new_csv_row_4.name == 'NewUserFromCsv'
    assert new_csv_row_4.age == 28

    print("Test passed: Data ingested correctly from CSV.")


def test_append_keeps_duplicate_keys(spark_session, create_test_table):
    data = pd.DataFrame({"id": ["1"], "name": ["Duplicate"], "email": ["dup@example.com"],
                         "signup_ts": ["2023-01-04T10:00:00.000Z"], "age": [22]})
    schema = StructType([
        StructField("id", StringType()), StructField("name", StringType()),
        StructField("email", StringType()), StructField("signup_ts", StringType()),
        StructField("age", IntegerType()),
    ])
    result = ingest_data_to_iceberg(data, schema, "pandas_df", {
        "table_name": create_test_table, "mode": "append",
    }, spark=spark_session)
    assert result.counts == {"received": 1, "accepted": 1, "quarantined": 0}
    assert spark_session.table(create_test_table).count() == 3


def test_overwrite_replaces_all_rows(spark_session, create_test_table):
    data = pd.DataFrame({"id": ["9"], "name": ["Replacement"], "email": ["nine@example.com"],
                         "signup_ts": ["2023-01-04T10:00:00.000Z"], "age": [22]})
    schema = StructType([
        StructField("id", StringType()), StructField("name", StringType()),
        StructField("email", StringType()), StructField("signup_ts", StringType()),
        StructField("age", IntegerType()),
    ])
    result = ingest_data_to_iceberg(data, schema, "pandas_df", {
        "table_name": create_test_table, "mode": "overwrite",
    }, spark=spark_session)
    assert result.counts == {"received": 1, "accepted": 1, "quarantined": 0}
    rows = spark_session.table(create_test_table).collect()
    assert [(row.id, row.name) for row in rows] == [("9", "Replacement")]


def test_empty_overwrite_clears_table(spark_session, create_test_table):
    schema = StructType([
        StructField("id", StringType()), StructField("name", StringType()),
        StructField("email", StringType()), StructField("signup_ts", StringType()),
        StructField("age", IntegerType()),
    ])
    data = pd.DataFrame(columns=["id", "name", "email", "signup_ts", "age"])
    result = ingest_data_to_iceberg(data, schema, "pandas_df", {
        "table_name": create_test_table, "mode": "overwrite",
    }, spark=spark_session)
    assert result.counts == {"received": 0, "accepted": 0, "quarantined": 0}
    assert spark_session.table(create_test_table).count() == 0


def test_invalid_rows_fail_without_changing_table(spark_session, create_test_table, tmp_path):
    path = tmp_path / "bad.csv"
    path.write_text("id,name,email,signup_ts,age\n3,Bad,bad@example.com,not-a-timestamp,30\n")
    schema = StructType([
        StructField("id", StringType()), StructField("name", StringType()),
        StructField("email", StringType()), StructField("signup_ts", StringType()),
        StructField("age", IntegerType()),
    ])
    with pytest.raises(ValueError, match="invalid row"):
        ingest_data_to_iceberg(str(path), schema, "csv", {"table_name": create_test_table}, spark=spark_session)
    assert spark_session.table(create_test_table).count() == 2


def test_invalid_rows_can_be_quarantined(spark_session, create_test_table, tmp_path):
    path = tmp_path / "bad.csv"
    path.write_text("id,name,email,signup_ts,age\n3,Bad,bad@example.com,not-a-timestamp,30\n")
    quarantine = str(tmp_path / "quarantine")
    schema = StructType([
        StructField("id", StringType()), StructField("name", StringType()),
        StructField("email", StringType()), StructField("signup_ts", StringType()),
        StructField("age", IntegerType()),
    ])
    result = ingest_data_to_iceberg(str(path), schema, "csv", {
        "table_name": create_test_table, "mode": "append",
        "invalid_rows": "quarantine", "quarantine_path": quarantine,
    }, spark=spark_session)
    assert result.counts == {"received": 1, "accepted": 0, "quarantined": 1}
    assert spark_session.read.parquet(quarantine).count() == 1
    assert spark_session.table(create_test_table).count() == 2


def test_additive_nullable_schema_evolution(spark_session, create_test_table):
    data = pd.DataFrame({"id": ["3"], "name": ["New"], "email": ["new@example.com"],
                         "signup_ts": ["2023-01-04T10:00:00.000Z"], "age": [31], "segment": ["gold"]})
    schema = StructType([
        StructField("id", StringType()), StructField("name", StringType()),
        StructField("email", StringType()), StructField("signup_ts", StringType()),
        StructField("age", IntegerType()), StructField("segment", StringType(), True),
    ])
    ingest_data_to_iceberg(data, schema, "pandas_df", {
        "table_name": create_test_table, "mode": "append", "evolve_schema": True,
    }, spark=spark_session)
    assert "segment" in spark_session.table(create_test_table).columns
    assert spark_session.table(create_test_table).where("id = '3'").first().segment == "gold"


def test_existing_column_type_change_is_rejected(spark_session, create_test_table):
    data = pd.DataFrame({"id": [3], "name": ["New"], "email": ["new@example.com"],
                         "signup_ts": ["2023-01-04T10:00:00.000Z"], "age": [31]})
    schema = StructType([
        StructField("id", IntegerType()), StructField("name", StringType()),
        StructField("email", StringType()), StructField("signup_ts", StringType()),
        StructField("age", IntegerType()),
    ])
    with pytest.raises(TypeError, match="Unsupported type change"):
        ingest_data_to_iceberg(data, schema, "pandas_df", {
            "table_name": create_test_table, "evolve_schema": True,
        }, spark=spark_session)
