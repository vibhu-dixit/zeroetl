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

def ingest_data_to_iceberg(input_data, schema: StructType, source_type: str, config: dict, *, spark=None):
    table_name = config.get("table_name", "mycatalog.db.users")
    primary_key = config.get("primary_key_col", "id")
    timestamp_col = config.get("timestamp_col", "signup_ts")
    warehouse_path = config.get("warehouse_path")
    spark_configs = config.get("spark_configs", {})
    
    owned = spark is None
    if owned:
        spark = get_spark_session(app_name="ZeroETLIngestion", spark_configs=spark_configs, warehouse_path=warehouse_path)

    try:
        if source_type == 'csv':
            csv_options = {"header": "true"}
            csv_options.update(config.get("csv_options", {}))
            reader = spark.read.schema(schema)
            for key, value in csv_options.items():
                reader = reader.option(key, value)
            df = reader.csv(input_data)
            print(f"Reading data from CSV at {input_data}...")
        elif source_type == 'pandas_df':
            df = spark.createDataFrame(input_data, schema=schema)
            print("Reading data from pandas DataFrame...")
        else:
            raise ValueError("Invalid source_type. Choose 'pandas_df' or 'csv'.")

        if isinstance(schema[timestamp_col].dataType, StringType):
            timestamp_format = config.get("timestamp_format", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
            df = df.withColumn(timestamp_col, to_timestamp(col(timestamp_col), timestamp_format) if timestamp_format else to_timestamp(col(timestamp_col)))

        print(f"Starting deduplication and merge for table {table_name}...")
        # Deduplication logic
        window_spec = Window.partitionBy(primary_key).orderBy(desc(timestamp_col))
        deduplicated_df = df.withColumn("row_num", row_number().over(window_spec))\
                                    .filter(col("row_num") == 1)\
                                    .drop("row_num")
        deduplicated_df.createOrReplaceTempView("zeroetl_incoming")
        spark.sql(f"""
            MERGE INTO {table_name} AS target
            USING zeroetl_incoming AS source
            ON target.{primary_key} = source.{primary_key}
            WHEN MATCHED AND source.{timestamp_col} > target.{timestamp_col}
              THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        print("Deduplication via overwrite completed successfully.")
    except Exception as e:
        print(f"Error during deduplication: {e}")
        raise
    finally:
        if owned:
            spark.stop()
