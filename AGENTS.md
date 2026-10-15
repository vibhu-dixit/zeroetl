# Agent guide

## What this package does

`zeroetl` is a Python API for loading pandas DataFrames or CSV files into Apache Iceberg tables with PySpark, plus table creation, querying, compaction, and snapshot expiration helpers. It also provides an optional local stdio MCP server (`zeroetl-mcp`) for agent hosts.

## Source map

- `zeroetl/ingestion.py`: `ingest_data_to_iceberg(...)` and Spark session setup for ingestion.
- `zeroetl/table_management.py`: `create_iceberg_table`, `query_iceberg_table`, `compact_iceberg_table`, `expire_iceberg_snapshots`, and Spark session setup for table operations.
- `zeroetl/__init__.py`: public package exports.
- `examples/run_ingestion.py`: example Python pipeline.
- `tests/test_ingestion.py`: integration tests for ingestion and table operations.

## Current API behavior and limits

- Ingestion accepts only `source_type="pandas_df"` or `source_type="csv"`; it requires a PySpark `StructType` schema.
- Ingestion deduplicates each input batch by the configured primary key, then merges into an existing Iceberg table. A matching row is updated only when the input timestamp is newer; new keys are inserted. Create the target table separately with `create_iceberg_table`.
- The Spark setup uses the `mycatalog` Hadoop catalog and reads its warehouse from the `warehouse_path` argument/config or `AWS_BUCKET_NAME`. S3 credentials are read from `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
- Table creation uses `CREATE TABLE IF NOT EXISTS`; it does not alter an existing table's schema. Schema migrations are outside the package's current API.
- Querying supports the latest table state or a snapshot ID time-travel query and returns up to 20 collected Spark rows.
- The MCP server exposes read-only query access by default; mutating tools require `ZEROETL_MCP_ENABLE_WRITES=true`.
- MCP CSV ingestion requires explicit path roots or URI prefixes in `ZEROETL_MCP_ALLOWED_INPUT_PREFIXES`.
- The MCP SDK is an optional extra installed with `pip install "zeroetl[mcp]"`; the base Python API does not import it.
- There is no REST service or generic connector interface. Do not describe these as supported features.

## Making changes

Keep changes within the existing Python API and its tests unless the requested task calls for a new interface. Check callers in `examples/` and tests when changing a public function or configuration key. Avoid claiming support for an input format or Iceberg operation unless implemented and covered by the code.

Run the test suite with:

```bash
python -m pytest -q
```

The integration tests start Spark and use Iceberg; run them in an environment with Java and the required Spark/Iceberg runtime available. A successful test collection alone does not verify the integration tests.
