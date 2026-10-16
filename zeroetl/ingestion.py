from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from uuid import uuid4

from .results import OperationResult
from .spark import get_spark_session


def ingest_data_to_iceberg(input_data, schema, source_type: str, config: dict, *, spark=None):
    """Ingest CSV or pandas data into Iceberg using upsert, append, or overwrite."""
    table_name = config.get("table_name", "mycatalog.db.users")
    primary_key = config.get("primary_key_col", "id")
    timestamp_col = config.get("timestamp_col", "signup_ts")
    mode = config.get("mode", "upsert")
    invalid_policy = config.get("invalid_rows", "fail")
    quarantine_path = config.get("quarantine_path")
    evolve_schema = config.get("evolve_schema", False)
    if mode not in ("upsert", "append", "overwrite"):
        raise ValueError("mode must be 'upsert', 'append', or 'overwrite'.")
    if invalid_policy not in ("fail", "quarantine"):
        raise ValueError("invalid_rows must be 'fail' or 'quarantine'.")
    if invalid_policy == "quarantine" and not quarantine_path:
        raise ValueError("quarantine_path is required when invalid_rows='quarantine'.")
    if not isinstance(evolve_schema, bool):
        raise ValueError("evolve_schema must be a boolean.")
    if source_type not in ("csv", "pandas_df"):
        raise ValueError("Invalid source_type. Choose 'pandas_df' or 'csv'.")
    if primary_key not in schema.fieldNames() or timestamp_col not in schema.fieldNames():
        raise ValueError("The schema must include the configured primary key and timestamp columns.")

    owned = spark is None
    if owned:
        spark = get_spark_session(
            app_name="ZeroETLIngestion",
            spark_configs=config.get("spark_configs", {}),
            warehouse_path=config.get("warehouse_path"),
        )
    try:
        if source_type == "csv":
            options = {"header": "true", **config.get("csv_options", {})}
            reader = spark.read.schema(schema)
            for key, value in options.items():
                reader = reader.option(key, value)
            df = reader.csv(input_data)
        else:
            df = spark.createDataFrame(input_data, schema=schema)

        if isinstance(schema[timestamp_col].dataType, StringType):
            timestamp_format = config.get("timestamp_format", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
            df = df.withColumn(
                timestamp_col,
                F.try_to_timestamp(F.col(timestamp_col), F.lit(timestamp_format))
                if timestamp_format else F.try_to_timestamp(F.col(timestamp_col)),
            )

        target_schema = spark.table(table_name).schema
        input_fields = {field.name: field for field in df.schema.fields}
        target_fields = {field.name: field for field in target_schema.fields}
        additions = []
        for name, field in input_fields.items():
            target = target_fields.get(name)
            if target is None:
                if not evolve_schema:
                    raise ValueError(f"Input column {name!r} is missing from the target table; enable evolve_schema to add it.")
                if not field.nullable:
                    raise ValueError(f"New column {name!r} must be nullable for schema evolution.")
                additions.append(field)
            elif field.dataType != target.dataType:
                raise TypeError(
                    f"Unsupported type change for column {name!r}: "
                    f"target is {target.dataType.simpleString()}, input is {field.dataType.simpleString()}."
                )

        missing = [field for name, field in target_fields.items() if name not in input_fields]
        required_missing = [field.name for field in missing if not field.nullable]
        if required_missing:
            raise ValueError(f"Input is missing required target columns: {', '.join(required_missing)}.")
        for field in missing:
            df = df.withColumn(field.name, F.lit(None).cast(field.dataType))

        received = df.count()
        valid = F.col(primary_key).isNotNull() & F.col(timestamp_col).isNotNull()
        invalid = df.filter(~valid)
        quarantined = invalid.count()
        if quarantined and invalid_policy == "fail":
            raise ValueError(
                f"Ingestion rejected {quarantined} invalid row(s): "
                f"{primary_key} and {timestamp_col} must be non-null and timestamps must parse."
            )
        if quarantined:
            invalid.withColumn(
                "_zeroetl_error",
                F.when(F.col(primary_key).isNull(), F.lit(f"null {primary_key}"))
                .otherwise(F.lit(f"null or invalid {timestamp_col}")),
            ).write.mode("append").parquet(quarantine_path)
        accepted_df = df.filter(valid)
        accepted = received - quarantined

        for field in additions:
            spark.sql(f"ALTER TABLE {table_name} ADD COLUMN {field.name} {field.dataType.simpleString()}")

        if accepted == 0 and mode in ("upsert", "append"):
            return OperationResult("ingest", table_name, counts={
                "received": received, "accepted": 0, "quarantined": quarantined,
            })

        # Align source ordering with the table schema so INSERT/MERGE * stays safe.
        target_names = [field.name for field in target_schema.fields] + [field.name for field in additions]
        accepted_df = accepted_df.select(*target_names)
        if mode == "append":
            accepted_df.writeTo(table_name).append()
        elif mode == "overwrite":
            accepted_df.writeTo(table_name).overwrite(F.lit(True))
        else:
            order = Window.partitionBy(primary_key).orderBy(
                F.desc(timestamp_col), F.desc(F.xxhash64(*[F.col(name) for name in target_names]))
            )
            deduplicated = accepted_df.withColumn("_zeroetl_rank", F.row_number().over(order)) \
                .filter(F.col("_zeroetl_rank") == 1).drop("_zeroetl_rank")
            view_name = f"zeroetl_incoming_{uuid4().hex}"
            deduplicated.createOrReplaceTempView(view_name)
            try:
                spark.sql(f"""
                    MERGE INTO {table_name} AS target
                    USING {view_name} AS source
                    ON target.{primary_key} = source.{primary_key}
                    WHEN MATCHED AND source.{timestamp_col} > target.{timestamp_col}
                      THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *
                """)
            finally:
                spark.catalog.dropTempView(view_name)

        return OperationResult("ingest", table_name, counts={
            "received": received, "accepted": accepted, "quarantined": quarantined,
        })
    finally:
        if owned:
            spark.stop()
