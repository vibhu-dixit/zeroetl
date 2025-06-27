import pandas as pd
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
from zeroetl.ingestion import ingest_data_to_iceberg
from zeroetl.table_management import create_iceberg_table, compact_iceberg_table, expire_iceberg_snapshots, query_iceberg_table
import os

# --- A. User-configurable settings ---
# Define a configuration dictionary for the ingestion process.
# Users can modify these values to change the behavior of the package.
INGESTION_CONFIG = {
    "table_name": "mycatalog.db.users",
    "primary_key_col": "id",
    "timestamp_col": "signup_ts",
    "warehouse_path": "s3a://zero-etl-mesh-demo/warehouse", # Optional: Override env variable
    "spark_configs": {
        "spark.sql.shuffle.partitions": "4", # Example of custom Spark config
        "spark.serializer.objectStreamReset": "100"
    }
}

# Define a configuration dictionary for table management tasks.
TABLE_MANAGEMENT_CONFIG = {
    "table_name": "mycatalog.db.users",
    "warehouse_path": "s3a://zero-etl-mesh-demo/warehouse", # Optional: Override env variable
    "spark_configs": {
        "spark.sql.shuffle.partitions": "4"
    },
    "older_than_seconds": 60, # For snapshot expiration
    "query_type": "latest", # 'latest' or 'time_travel'
    "snapshot_id": None # For time travel queries
}

# --- B. User-defined schema ---
# This is where the user defines the schema of their input data.
user_defined_schema = StructType() \
    .add("id", StringType()) \
    .add("name", StringType()) \
    .add("email", StringType()) \
    .add("signup_ts", StringType()) \
    .add("age", IntegerType())

# --- C. Main execution logic ---
def run_pipeline():
    """
    Main function to run the ingestion and management pipeline.
    """
    # 1. Create the Iceberg table if it doesn't exist.
    # This uses the logic from your open_iceberg_shell.py file.
    create_table_config = {
        "table_name": INGESTION_CONFIG["table_name"],
        "schema": "id STRING, name STRING, email STRING, signup_ts TIMESTAMP, age INT",
        "partition_spec": "days(signup_ts), bucket(16, id)",
        "location": "s3a://zero-etl-mesh-demo/warehouse/db/users",
        "warehouse_path": INGESTION_CONFIG["warehouse_path"]
    }
    create_iceberg_table(create_table_config)

    print("\n--- Ingesting data from a pandas DataFrame ---")
    pandas_data = pd.DataFrame({
        'id': ['1', '2', '4'],
        'name': ['Alice', 'Bob', 'Charlie'],
        'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com'],
        'signup_ts': ['2023-01-01T12:00:00.000Z', '2023-01-02T13:00:00.000Z', '2023-01-04T15:00:00.000Z'],
        'age': [25, 30, 35]
    })
    
    try:
        ingest_data_to_iceberg(
            input_data=pandas_data,
            schema=user_defined_schema,
            source_type='pandas_df',
            config=INGESTION_CONFIG
        )
        print("Pandas DataFrame ingested successfully.")
    except Exception as e:
        print(f"Error during pandas ingestion: {e}")

    # 3. Ingest data from a CSV file (uncomment to run).
    # print("\n--- Ingesting data from a CSV file ---")
    # csv_file_path = 'path/to/your/users.csv'
    # try:
    #     ingest_data_to_iceberg(
    #         input_data=csv_file_path,
    #         schema=user_defined_schema,
    #         source_type='csv',
    #         config=INGESTION_CONFIG
    #     )
    #     print("CSV file ingested successfully.")
    # except Exception as e:
    #     print(f"Error during CSV ingestion: {e}")

    # 4. Run management tasks using the flexible configuration.
    print("\n--- Querying the latest state of the table ---")
    query_iceberg_table(TABLE_MANAGEMENT_CONFIG)

    # print("\n--- Running Iceberg table compaction ---")
    # compact_iceberg_table(TABLE_MANAGEMENT_CONFIG)

    # print("\n--- Expiring old snapshots ---")
    # expire_iceberg_snapshots(TABLE_MANAGEMENT_CONFIG)

if __name__ == "__main__":
    run_pipeline()