# ZeroETL adoption feature research

**Researched:** 2026-10-14

## What the evidence can and cannot say

Public PyPI download counts do not identify whether an install came from a person, an AI agent, CI, or an automated mirror. They are a rough package-ecosystem signal, not a count of agent adoption or unique users. The PyPI project reports that metadata requests were removed from download counts in August 2026, improving the measure, while it remains a download count rather than a user count: <https://blog.pypi.org/posts/2026-08-31-download-counts/>.

At the time of research, PyPI Stats showed about 47.8 million PySpark downloads, 20.3 million PyIceberg downloads, 54.9 million DuckDB downloads, and 4.9 million dlt downloads in the prior month. These indicate substantial activity around Spark, Iceberg, local analytics, and Python ingestion tooling. They do not establish which features caused downloads: <https://pypistats.org/packages/pyspark>, <https://pypistats.org/packages/pyiceberg>, <https://pypistats.org/packages/duckdb>, <https://pypistats.org/packages/dlt>.

For agent compatibility, the MIT AI Agent Index reports that 20 of 30 profiled agents support MCP for tool integration. dbt's official MCP documentation describes exposing governed metadata, freshness, queries, and CLI/API actions to AI tools. These are stronger signals for MCP as an integration format than PyPI package counts: <https://aiagentindex.mit.edu/data/2025-AI-Agent-Index.pdf>, <https://docs.getdbt.com/docs/dbt-ai/about-mcp>.

## Feature patterns in adjacent ingestion and Iceberg projects

- dlt documents append, merge, and full-refresh write modes, cursor-based incremental loading, persisted state, schema evolution, and a CLI. Its REST, SQL database, and filesystem sources cover a broad range of ingestion jobs: <https://dlthub.com/docs/general-usage/incremental-loading>, <https://dlthub.com/docs/general-usage/incremental/cursor>, <https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api>.
- Iceberg supports a REST catalog protocol intended to let different clients use one common catalog API. Supporting it would remove ZeroETL's current Hadoop catalog assumption and make the package easier to connect to managed catalogs: <https://iceberg.apache.org/docs/latest/rest-catalog/>.
- Iceberg supports branch and tag references for isolated writes, audit workflows, and retained historical snapshots. Exposing these operations could add safe validation and release workflows around table updates: <https://iceberg.apache.org/docs/latest/branching/>.
- Iceberg Spark writes cover append, overwrite, and `MERGE INTO`; merge is recommended for row-level changes because it limits replacement to affected data files: <https://iceberg.apache.org/docs/latest/spark-writes/>.
- `uv` supports installing a package's CLI tool into an isolated environment or running it ephemerally. A stable `zeroetl` CLI therefore supports both human workflows and agent execution without requiring a separate runtime service: <https://docs.astral.sh/uv/reference/cli/>.

## Recommended additions after the approved scope

1. **Optional MCP adapter.** Build it on top of the CLI/API rather than duplicating pipeline code. Start with read-only tools for catalog/table discovery, schema and snapshot inspection, plus `validate` and `plan` tools. Gate write, overwrite, and snapshot-expiration tools behind explicit configuration and dry-run support. This is the most direct agent-facing expansion.
2. **Catalog configuration and Iceberg REST support.** Replace hardcoded Hadoop/S3 session assumptions with named catalog configuration, then support REST catalogs. This is the most valuable interoperability expansion for teams already using Iceberg.
3. **Common file and in-memory formats.** Add Parquet, JSONL, Arrow, and Spark DataFrame input before building a large connector catalog. These complement current CSV/Pandas input and fit Spark's distributed processing model.
4. **Incremental cursor state and CDC deletes.** Persist watermarks so upstream jobs can request only changed rows. Extend upsert to represent deletes and change events. This can avoid repeatedly loading full source tables but needs explicit state storage and restart semantics.
5. **Branch/tag workflows and data quality checks.** Allow writes to an audit branch, validate configured constraints, and promote a branch after checks pass. This builds on Iceberg's native snapshot references and makes agent-triggered changes more reviewable.

## Suggested sequence

Finish the approved structured API, ingestion modes, JSON CLI, and local quickstart first. Then add catalog configuration and additional file inputs. Build the optional MCP adapter on the stable CLI/API, followed by cursor state, CDC, and branch-based validation. Keep the connector surface optional so users do not need every source dependency installed.
