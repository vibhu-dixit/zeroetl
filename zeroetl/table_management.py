import os
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from dotenv import load_dotenv

def get_spark_session(app_name: str, spark_configs: dict = None, warehouse_path: str = None):
    load_dotenv()
    
    final_warehouse_path = warehouse_path or os.getenv("AWS_BUCKET_NAME")
    if not final_warehouse_path:
        raise ValueError("Warehouse path not provided and AWS_BUCKET_NAME environment variable not set.")

    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.mycatalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.mycatalog.type", "hadoop") \
        .config("spark.sql.catalog.mycatalog.warehouse", final_warehouse_path) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.sql.default.catalog", "mycatalog")
    
    if spark_configs:
        for key, value in spark_configs.items():
            builder = builder.config(key, value)
            
    return builder.getOrCreate()

def create_iceberg_table(table_config: dict):
    table_name = table_config.get("table_name", "mycatalog.db.users")
    schema_str = table_config.get("schema", "id STRING, name STRING, email STRING, signup_ts TIMESTAMP, age INT")
    partition_spec = table_config.get("partition_spec", "days(signup_ts), bucket(16, id)")
    location = table_config.get("location", f"s3a://zero-etl-mesh-demo/warehouse/db/{table_name.split('.')[-1]}")
    warehouse_path = table_config.get("warehouse_path")
    spark_configs = table_config.get("spark_configs", {})
    
    spark = get_spark_session(app_name="IcebergTableCreation", spark_configs=spark_configs, warehouse_path=warehouse_path)
    
    create_table_sql = f"""
        CREATE OR REPLACE TABLE {table_name} (
            {schema_str}
        )
        USING iceberg
        PARTITIONED BY ({partition_spec})
        LOCATION '{location}'
        TBLPROPERTIES ('format-version'='2')
    """
    print(f"Creating new table '{table_name}' with schema and partitioning...")
    spark.sql(create_table_sql)
    print(f"New partitioned table '{table_name}' created successfully.")
    spark.sql(f"DESCRIBE TABLE {table_name};").show()
    spark.stop()

def expire_iceberg_snapshots(table_config: dict):
    table_name = table_config.get("table_name", "mycatalog.db.users")
    older_than_seconds = table_config.get("older_than_seconds", 30)
    warehouse_path = table_config.get("warehouse_path")
    spark_configs = table_config.get("spark_configs", {})

    spark = get_spark_session(app_name="IcebergSnapshotExpiration", spark_configs=spark_configs, warehouse_path=warehouse_path)
    spark.sparkContext.setLogLevel("WARN")
    expiration_timestamp = (datetime.now() - timedelta(seconds=older_than_seconds)).isoformat(timespec='milliseconds') + 'Z'

    print(f"Expiring snapshots in {table_name} older than {expiration_timestamp}...")
    spark.sql(f"CALL mycatalog.system.expire_snapshots(table => '{table_name}', older_than => TIMESTAMP '{expiration_timestamp}')").show()

    print("Verifying history after expiration")
    spark.sql(f"SELECT * FROM {table_name}.history ORDER BY made_current_at DESC").show(truncate=False)
    spark.stop()

def compact_iceberg_table(table_config: dict):
    """
    Compacts an Iceberg table by rewriting data files.
    """
    table_name = table_config.get("table_name", "mycatalog.db.users")
    warehouse_path = table_config.get("warehouse_path")
    spark_configs = table_config.get("spark_configs", {})

    spark = get_spark_session(app_name="IcebergTableCompaction", spark_configs=spark_configs, warehouse_path=warehouse_path)
    try:
        print(f"Starting Iceberg table compaction for {table_name}...")
        spark.sql(f"CALL mycatalog.system.rewrite_data_files('{table_name}')")
        print("Iceberg table compaction completed.")
    except Exception as e:
        print(f"Error during compaction: {e}")
    finally:
        spark.stop()

def query_iceberg_table(table_config: dict):
    table_name = table_config.get("table_name", "mycatalog.db.users")
    query_type = table_config.get("query_type", "latest")
    snapshot_id = table_config.get("snapshot_id")
    warehouse_path = table_config.get("warehouse_path")
    spark_configs = table_config.get("spark_configs", {})

    spark = get_spark_session(app_name="QueryUsers", spark_configs=spark_configs, warehouse_path=warehouse_path)
    try:
        if query_type == 'time_travel' and snapshot_id:
            print(f"--- Time travel to a specific snapshot using VERSION AS OF {snapshot_id} ---")
            spark.sql(f"SELECT * FROM {table_name} VERSION AS OF {snapshot_id}").show(truncate=False)
        elif query_type == 'latest':
            print("--- Querying the latest state of the table ---")
            spark.sql(f"SELECT * FROM {table_name}").show(truncate=False)
        else:
            print("Invalid query type or missing snapshot ID.")
    except Exception as e:
        print(f"Error querying table: {e}")
    finally:
        spark.stop()