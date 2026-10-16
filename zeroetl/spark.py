"""Shared Spark session configuration for Iceberg operations."""

import os
from typing import Optional

from dotenv import load_dotenv
from pyspark.sql import SparkSession


def get_spark_session(app_name: str, spark_configs: Optional[dict] = None,
                      warehouse_path: Optional[str] = None) -> SparkSession:
    load_dotenv()
    final_warehouse_path = warehouse_path or os.getenv("AWS_BUCKET_NAME")
    if not final_warehouse_path:
        raise ValueError("Warehouse path not provided and AWS_BUCKET_NAME environment variable not set.")

    configs = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.mycatalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.mycatalog.type": "hadoop",
        "spark.sql.catalog.mycatalog.warehouse": final_warehouse_path,
        "spark.hadoop.fs.s3a.endpoint": "s3.amazonaws.com",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.sql.default.catalog": "mycatalog",
    }
    for env_name, spark_key in (
        ("AWS_ACCESS_KEY_ID", "spark.hadoop.fs.s3a.access.key"),
        ("AWS_SECRET_ACCESS_KEY", "spark.hadoop.fs.s3a.secret.key"),
    ):
        if os.getenv(env_name):
            configs[spark_key] = os.getenv(env_name)
    configs.update(spark_configs or {})
    builder = SparkSession.builder.appName(app_name)
    for key, value in configs.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
