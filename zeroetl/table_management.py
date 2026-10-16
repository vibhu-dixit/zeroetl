from datetime import datetime, timedelta
from .results import OperationResult
from .spark import get_spark_session


def _procedure_counts(rows):
    if not rows:
        return None
    values = rows[0].asDict(recursive=True)
    counts = {key: value for key, value in values.items() if "count" in key.lower()}
    return counts or None


def create_iceberg_table(table_config: dict, *, spark=None):
    table_name = table_config.get("table_name", "mycatalog.db.users")
    schema_str = table_config.get("schema", "id STRING, name STRING, email STRING, signup_ts TIMESTAMP, age INT")
    partition_spec = table_config.get("partition_spec", "days(signup_ts), bucket(16, id)")
    location = table_config.get("location", f"s3a://zero-etl-mesh-demo/warehouse/db/{table_name.split('.')[-1]}")
    warehouse_path = table_config.get("warehouse_path")
    spark_configs = table_config.get("spark_configs", {})
    
    owned = spark is None
    if owned:
        spark = get_spark_session(app_name="IcebergTableCreation", spark_configs=spark_configs, warehouse_path=warehouse_path)
    
    location_clause = f"LOCATION '{location}'" if location else ""
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {schema_str}
        )
        USING iceberg
        PARTITIONED BY ({partition_spec})
        {location_clause}
        TBLPROPERTIES ('format-version'='2')
    """
    try:
        print(f"Creating new table '{table_name}' with schema and partitioning...")
        spark.sql(create_table_sql)
        print(f"New partitioned table '{table_name}' created successfully.")
        spark.sql(f"DESCRIBE TABLE {table_name};").show()
        return OperationResult("create_table", table_name)
    finally:
        if owned:
            spark.stop()

def expire_iceberg_snapshots(table_config: dict, *, spark=None):
    table_name = table_config.get("table_name", "mycatalog.db.users")
    older_than_seconds = table_config.get("older_than_seconds", 30)
    warehouse_path = table_config.get("warehouse_path")
    spark_configs = table_config.get("spark_configs", {})

    owned = spark is None
    if owned:
        spark = get_spark_session(app_name="IcebergSnapshotExpiration", spark_configs=spark_configs, warehouse_path=warehouse_path)
    try:
        spark.sparkContext.setLogLevel("WARN")
        expiration_timestamp = (datetime.now() - timedelta(seconds=older_than_seconds)).isoformat(timespec='milliseconds') + 'Z'

        print(f"Expiring snapshots in {table_name} older than {expiration_timestamp}...")
        expired = spark.sql(f"CALL mycatalog.system.expire_snapshots(table => '{table_name}', older_than => TIMESTAMP '{expiration_timestamp}')").collect()

        print("Verifying history after expiration")
        spark.sql(f"SELECT * FROM {table_name}.history ORDER BY made_current_at DESC").show(truncate=False)
        return OperationResult("expire_snapshots", table_name, counts=_procedure_counts(expired))
    finally:
        if owned:
            spark.stop()

def compact_iceberg_table(table_config: dict, *, spark=None):
    """
    Compacts an Iceberg table by rewriting data files.
    """
    table_name = table_config.get("table_name", "mycatalog.db.users")
    warehouse_path = table_config.get("warehouse_path")
    spark_configs = table_config.get("spark_configs", {})

    owned = spark is None
    if owned:
        spark = get_spark_session(app_name="IcebergTableCompaction", spark_configs=spark_configs, warehouse_path=warehouse_path)
    try:
        print(f"Starting Iceberg table compaction for {table_name}...")
        stats = spark.sql(f"CALL mycatalog.system.rewrite_data_files('{table_name}')").collect()
        print("Iceberg table compaction completed.")
        return OperationResult("compact", table_name, counts=_procedure_counts(stats))
    except Exception as e:
        print(f"Error during compaction: {e}")
        raise
    finally:
        if owned:
            spark.stop()

def query_iceberg_table(table_config: dict, *, spark=None):
    table_name = table_config.get("table_name", "mycatalog.db.users")
    query_type = table_config.get("query_type", "latest")
    snapshot_id = table_config.get("snapshot_id")
    warehouse_path = table_config.get("warehouse_path")
    spark_configs = table_config.get("spark_configs", {})

    owned = spark is None
    if owned:
        spark = get_spark_session(app_name="QueryUsers", spark_configs=spark_configs, warehouse_path=warehouse_path)
    try:
        if query_type == 'time_travel' and type(snapshot_id) is int and snapshot_id > 0:
            print(f"--- Time travel to a specific snapshot using VERSION AS OF {snapshot_id} ---")
            result = spark.sql(f"SELECT * FROM {table_name} VERSION AS OF {snapshot_id}").limit(20).collect()
        elif query_type == 'latest':
            print("--- Querying the latest state of the table ---")
            result = spark.sql(f"SELECT * FROM {table_name}").limit(20).collect()
        else:
            raise ValueError("Invalid query type or missing snapshot ID.")
        print(result)
        rows = [row.asDict(recursive=True) for row in result]
        return OperationResult("query", table_name, counts={"returned": len(rows)}, rows=rows)
    except Exception as e:
        print(f"Error querying table: {e}")
        raise
    finally:
        if owned:
            spark.stop()
