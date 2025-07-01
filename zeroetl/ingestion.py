import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, row_number, to_timestamp
from pyspark.sql.types import StructType, StringType
from pyspark.sql.window import Window
from dotenv import load_dotenv

def get_spark_session(app_name: str, spark_configs: dict = None, warehouse_path: str = None):
    load_dotenv()

    final_warehouse_path = warehouse_path or os.getenv("AWS_BUCKET_NAME")
    if not final_warehouse_path:
        raise ValueError("Warehouse path not provided and AWS_BUCKET_NAME environment variable not set.")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.mycatalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.mycatalog.type", "hadoop")
        .config("spark.sql.catalog.mycatalog.warehouse", final_warehouse_path)
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.sql.default.catalog", "mycatalog")
    )
    if spark_configs:
        for key, value in spark_configs.items():
            builder = builder.config(key, value)
            
    return builder.getOrCreate()

def ingest_data_to_iceberg(input_data, schema: StructType, source_type: str, config: dict):
    table_name = config.get("table_name", "mycatalog.db.users")
    primary_key = config.get("primary_key_col", "id")
    timestamp_col = config.get("timestamp_col", "signup_ts")
    warehouse_path = config.get("warehouse_path")
    spark_configs = config.get("spark_configs", {})
    
    spark = get_spark_session(app_name="ZeroETLIngestion", spark_configs=spark_configs, warehouse_path=warehouse_path)

    if source_type == 'csv':
        df = spark.read.schema(schema).csv(input_data)
        print(f"Reading data from CSV at {input_data}...")
    elif source_type == 'pandas_df':
        df = spark.createDataFrame(input_data, schema=schema)
        print("Reading data from pandas DataFrame...")
    else:
        raise ValueError("Invalid source_type. Choose 'pandas_df' or 'csv'.")

    # The signup_ts column from the input is a string, convert it to TimestampType.
    if isinstance(schema[timestamp_col].dataType, StringType):
        df = df.withColumn(timestamp_col, to_timestamp(col(timestamp_col), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

    print(f"Starting deduplication via overwrite for table {table_name}...")
    try:
        # Deduplication logic
        existing_table_df = spark.table(table_name)
        combined_df = existing_table_df.unionByName(df, allowMissingColumns=True)
        window_spec = Window.partitionBy(primary_key).orderBy(desc(timestamp_col))
        deduplicated_df = combined_df.withColumn("row_num", row_number().over(window_spec))\
                                    .filter(col("row_num") == 1)\
                                    .drop("row_num")

        deduplicated_df.writeTo(table_name) \
            .using("iceberg") \
            .overwritePartitions()

        print("Deduplication via overwrite completed successfully.")
    except Exception as e:
        print(f"Error during deduplication: {e}")
        raise
    finally:
        spark.stop()
