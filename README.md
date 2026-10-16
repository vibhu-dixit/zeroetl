# zeroetl

`zeroetl` is a Python package for loading CSV files and pandas DataFrames into Apache Iceberg tables with PySpark. It provides timestamp-based upserts, table creation, snapshot expiration, file compaction, and snapshot-aware queries for Python data pipelines.

[Install from PyPI](https://pypi.org/project/zeroetl/) · [Source code](https://github.com/vibhu-dixit/zeroetl) · [Report an issue](https://github.com/vibhu-dixit/zeroetl/issues)

Use `zeroetl` when a Python or Spark pipeline needs to ingest batch data into Iceberg and apply updates by primary key and timestamp. It provides a Python API and an optional local MCP server for agent hosts.

## Capabilities

- Read input from pandas DataFrames or CSV files.
- Create partitioned Iceberg tables without replacing existing tables.
- Ingest in `upsert` (default), `append`, or explicit full-table `overwrite` mode.
- Validate required keys and timestamps; fail on invalid rows by default or append them to an explicitly configured quarantine path.
- Opt into additive schema evolution for new nullable columns; incompatible type changes are rejected.
- Query the latest table state or read a specific Iceberg snapshot.
- Compact data files and expire old table snapshots.
- Reuse caller-owned Spark sessions and return structured `OperationResult` values.
- Run the same operations through the JSON `zeroetl` CLI with dry-run support.
- Connect a local AI agent host through the optional stdio MCP server. It exposes read-only queries by default; write operations require explicit opt-in.

The package configures a Hadoop Iceberg catalog named `mycatalog`. Spark and a compatible Iceberg Spark runtime must be available to the application. Set the warehouse with `warehouse_path` or `AWS_BUCKET_NAME`; for S3 warehouses, set credentials with `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. Local filesystem warehouse paths do not require AWS credentials.

## Python API

| Function | Purpose |
| --- | --- |
| `ingest_data_to_iceberg` | Ingest a pandas DataFrame or CSV file and upsert by key and timestamp. |
| `create_iceberg_table` | Create an Iceberg table if it does not already exist. |
| `query_iceberg_table` | Return up to 20 rows from the current table state or a selected snapshot. |
| `compact_iceberg_table` | Rewrite data files to compact an Iceberg table. |
| `expire_iceberg_snapshots` | Remove snapshots older than the configured retention interval. |

All operations accept an optional keyword-only `spark` session. A supplied session stays running and remains owned by the caller; when omitted, ZeroETL creates and stops a session. Each successful operation returns an `OperationResult` with `operation`, `table_name`, `status`, optional `counts`, optional query `rows`, and `message`. Call `result.to_dict()` to get JSON-safe values. Ingestion counts include `received`, `accepted`, and `quarantined`; query counts include `returned`.

Ingestion options live in the existing config dictionary: `mode` (`upsert`, `append`, or `overwrite`), `evolve_schema` (default `false`), `invalid_rows` (`fail` by default or `quarantine`), and `quarantine_path` (required for quarantine). Empty append and upsert inputs do nothing; an explicitly selected empty overwrite clears the table. Quarantine files are written in Parquet format and appended to the configured path.

See the [quick start](#quick-start) and [`examples/run_ingestion.py`](examples/run_ingestion.py) for Python usage.

## Command line

The `zeroetl` command reads a JSON config with `spark` and `table` objects; ingestion also uses an `ingestion` object. For example:

```json
{
  "spark": {
    "warehouse_path": "file:///data/iceberg-warehouse",
    "spark_configs": {"spark.master": "local[*]"}
  },
  "table": {
    "table_name": "mycatalog.db.users",
    "schema_sql": "id STRING, signup_ts TIMESTAMP, name STRING",
    "partition_spec": "days(signup_ts)",
    "location": null
  },
  "ingestion": {
    "input_data": "users.csv",
    "source_type": "csv",
    "schema": [
      {"name": "id", "type": "string", "nullable": false},
      {"name": "signup_ts", "type": "string", "nullable": false},
      {"name": "name", "type": "string"}
    ],
    "primary_key_col": "id",
    "timestamp_col": "signup_ts",
    "mode": "upsert"
  }
}
```

Supported commands are `check`, `create-table`, `ingest`, `query`, `compact`, and `expire-snapshots`. Use `--dry-run` to validate and print the planned operation without starting Spark or changing table data. Use `--output json` for machine-readable results; failures return a nonzero exit code and a structured error.

```bash
zeroetl --config zeroetl.json --dry-run --output json ingest
zeroetl --config zeroetl.json create-table
zeroetl --config zeroetl.json --output json ingest
zeroetl --config zeroetl.json query --output json
zeroetl --config zeroetl.json compact
zeroetl --config zeroetl.json expire-snapshots
```

The CLI schema `type` accepts `string`, `int`/`integer`, `bigint`/`long`, `float`, `double`, `boolean`, `date`, `timestamp`, and `binary`. Pandas input can be supplied as a JSON array of objects; CSV input uses a path. Set `table.location` to `null` to let Iceberg derive the table path from the configured warehouse.

## Installation

### Prerequisites

- Python 3.10+
- `pip` (Python package installer)
- A Spark installation with a compatible Apache Iceberg runtime configured.
- An Iceberg warehouse location. The included Spark configuration uses the `s3a://` protocol; provide S3 credentials through environment variables or a `.env` file (see [Configuration](#configuration)).

### Install via Pip (Recommended for Users)

```bash
python -m venv venv
source venv/bin/activate  # On Windows: `venv\Scripts\activate`
pip install zeroetl
```

To install the local MCP server support:

```bash
pip install "zeroetl[mcp]"
zeroetl-mcp
```

Configure your agent host to launch `zeroetl-mcp` as an MCP server using the `stdio` transport. The process reads its warehouse and AWS credentials from the environment, just like the Python API. By default, it exposes `query_table` only. Set `ZEROETL_MCP_ENABLE_WRITES=true` to expose table creation, CSV ingestion, compaction, and snapshot expiration. Before enabling CSV ingestion, set `ZEROETL_MCP_ALLOWED_INPUT_PREFIXES` to comma-separated local directory roots or URI prefixes the agent may read. Inputs outside those prefixes are rejected. Never place credentials in tool arguments.

Example MCP client configuration:

```json
{
  "mcpServers": {
    "zeroetl": {
      "command": "zeroetl-mcp",
      "args": [],
      "env": {
        "AWS_BUCKET_NAME": "s3a://your-iceberg-warehouse-bucket/"
      }
    }
  }
}
```
### Install from Source
```bash
git clone https://github.com/vibhu-dixit/zeroetl.git
cd zeroetl
python -m venv venv
source venv/bin/activate  # On Windows: `venv\Scripts\activate`
pip install -e .
```

For the local MCP server, install the optional extra instead:

```bash
pip install -e '.[mcp]'
```
## Configuration
Create a `.env` file in your project directory with the following:
```plaintext
AWS_ACCESS_KEY_ID=YOUR_AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY=YOUR_AWS_SECRET_ACCESS_KEY
AWS_BUCKET_NAME=s3a://your-iceberg-warehouse-bucket/
```
Table creation is non-destructive: an existing table is left as-is. Manage table schema changes separately.
## Quick Start

1. **Set up your environment:**

- Install zeroetl (see Installation).
- Create a .env file (see Configuration).
- Prepare a sample users.csv (e.g., in examples/users.csv):
```csv
id,name,email,signup_ts,age
5,David,david@example.com,2023-01-05T16:00:00.000Z,40
6,Eve,eve@example.com,2023-01-06T17:00:00.000Z,45
7,Frank,frank@example.com,2023-01-07T18:00:00.000Z,50
```
2. **Create a pipeline script (e.g., `my_pipeline.py`):**
```python
import pandas as pd
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
from zeroetl.ingestion import ingest_data_to_iceberg
from zeroetl.table_management import create_iceberg_table, query_iceberg_table
import os

# Ingestion configuration
INGESTION_CONFIG = {
    "table_name": "mycatalog.db.users",
    "primary_key_col": "id",
    "timestamp_col": "signup_ts",
    "warehouse_path": os.getenv("AWS_BUCKET_NAME"),
    "spark_configs": {"spark.sql.shuffle.partitions": "4"}
}

# Table management configuration
TABLE_MANAGEMENT_CONFIG = {
    "table_name": "mycatalog.db.users",
    "warehouse_path": os.getenv("AWS_BUCKET_NAME"),
    "spark_configs": {"spark.sql.shuffle.partitions": "4"},
    "query_type": "latest"
}

# Input schema
user_defined_schema = StructType() \
    .add("id", StringType()) \
    .add("name", StringType()) \
    .add("email", StringType()) \
    .add("signup_ts", StringType()) \
    .add("age", IntegerType())

# Table creation configuration
CREATE_TABLE_CONFIG = {
    "table_name": INGESTION_CONFIG["table_name"],
    "schema": "id STRING, name STRING, email STRING, signup_ts TIMESTAMP, age INT",
    "partition_spec": "days(signup_ts), bucket(16, id)",
    "location": "s3a://zero-etl-mesh-demo/warehouse/db/users",
    "warehouse_path": INGESTION_CONFIG["warehouse_path"]
}

def run_pipeline():
    try:
        # Create Iceberg table
        print(f"Creating table: {CREATE_TABLE_CONFIG['table_name']}")
        create_iceberg_table(CREATE_TABLE_CONFIG)

        # Ingest data from Pandas DataFrame
        pandas_data = pd.DataFrame({
            'id': ['1', '2', '4'],
            'name': ['Alice', 'Bob', 'Charlie'],
            'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com'],
            'signup_ts': ['2023-01-01T12:00:00.000Z', '2023-01-02T13:00:00.000Z', '2023-01-04T15:00:00.000Z'],
            'age': [25, 30, 35]
        })
        ingest_data_to_iceberg(
            input_data=pandas_data,
            schema=user_defined_schema,
            source_type='pandas_df',
            config=INGESTION_CONFIG
        )

        # Ingest data from CSV
        csv_file_path = 'examples/users.csv'
        ingest_data_to_iceberg(
            input_data=csv_file_path,
            schema=user_defined_schema,
            source_type='csv',
            config=INGESTION_CONFIG
        )

        # Query table
        query_iceberg_table(TABLE_MANAGEMENT_CONFIG)

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_pipeline()
```
3. **Run pipeline**
```bash
python my_pipeline.py
```
