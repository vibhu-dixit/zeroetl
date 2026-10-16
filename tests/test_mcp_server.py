import asyncio
import builtins
from datetime import datetime
import importlib
import json
from unittest.mock import MagicMock

import pytest
pytest.importorskip("mcp.server.mcpserver")
import anyio
from mcp import ClientSession
from mcp.shared.memory import create_client_server_memory_streams

from zeroetl import mcp_server
from zeroetl.results import OperationResult


@pytest.mark.parametrize("table", ["db.users", "mycatalog.db.users; DROP TABLE x", "mycatalog.db.bad-name"])
def test_rejects_unsafe_table_identifiers(table):
    with pytest.raises(ValueError):
        mcp_server.validate_table(table)


def test_rejects_unsafe_schema_and_partition():
    with pytest.raises(ValueError):
        mcp_server.validate_schema([{"name": "id); DROP TABLE x", "type": "STRING"}])
    with pytest.raises(ValueError):
        mcp_server.validate_partition(["bucket(0, id)"])


def test_schema_rejects_non_string_column_name():
    with pytest.raises(ValueError):
        mcp_server.validate_schema([{"name": 1, "type": "STRING"}])


def test_nonfinite_query_values_are_strict_json_safe():
    assert json.dumps(mcp_server._json_safe({"value": float("nan")}), allow_nan=False) == '{"value": "NaN"}'


def test_binary_query_values_are_strict_json_safe():
    assert json.dumps(mcp_server._json_safe({"value": bytearray(b"\x00\xff")}), allow_nan=False) == '{"value": "AP8="}'


@pytest.mark.parametrize("snapshot", [0, -1, True, "1; DROP TABLE x"])
def test_rejects_bad_snapshot_ids(snapshot):
    with pytest.raises(ValueError):
        mcp_server.validate_snapshot(snapshot)


@pytest.mark.parametrize("seconds", [0, -1, 10**12, True])
def test_rejects_bad_retention(seconds):
    with pytest.raises(ValueError):
        mcp_server.validate_retention(seconds)


def test_csv_path_must_be_within_allowed_root(tmp_path, monkeypatch):
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    monkeypatch.setenv("ZEROETL_MCP_ALLOWED_INPUT_PREFIXES", str(allowed))
    assert mcp_server.validate_path(str(allowed / "input.csv")) == str(allowed / "input.csv")
    with pytest.raises(ValueError):
        mcp_server.validate_path(str(tmp_path / "allowed-other" / "input.csv"))
    with pytest.raises(ValueError):
        mcp_server.validate_path(str(allowed / ".." / "outside.csv"))


def test_csv_uri_rejects_encoded_traversal(monkeypatch):
    monkeypatch.setenv("ZEROETL_MCP_ALLOWED_INPUT_PREFIXES", "s3a://bucket/allowed")
    with pytest.raises(ValueError):
        mcp_server.validate_path("s3a://bucket/allowed/%2e%2e/outside.csv")


@pytest.mark.parametrize("suffix", ["{input.csv,../secret.csv}", "*.csv", "?.csv", "[ab].csv", r"\*.csv"])
def test_csv_path_rejects_spark_globs_before_read(tmp_path, monkeypatch, suffix):
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    monkeypatch.setenv("ZEROETL_MCP_ALLOWED_INPUT_PREFIXES", str(allowed))
    with pytest.raises(ValueError, match="glob"):
        mcp_server.validate_path(str(allowed / suffix))


def test_server_registers_only_query_by_default(monkeypatch):
    monkeypatch.delenv("ZEROETL_MCP_ENABLE_WRITES", raising=False)
    server = mcp_server.create_server()
    assert [tool.name for tool in asyncio.run(server.list_tools())] == ["query_table"]


def test_module_gives_install_hint_without_mcp_or_typing_extensions(monkeypatch):
    real_import = builtins.__import__

    def without_extra(name, *args, **kwargs):
        if name in {"mcp.server.mcpserver", "typing_extensions"}:
            raise ImportError(name)
        return real_import(name, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(builtins, "__import__", without_extra)
        module = importlib.reload(mcp_server)
        with pytest.raises(RuntimeError, match=r"pip install zeroetl\[mcp\]"):
            module.create_server()
    importlib.reload(mcp_server)


def test_server_registers_writes_only_with_explicit_opt_in(monkeypatch):
    monkeypatch.setenv("ZEROETL_MCP_ENABLE_WRITES", "true")
    server = mcp_server.create_server()
    assert {tool.name for tool in asyncio.run(server.list_tools())} == {
        "query_table", "ingest_csv", "create_table", "compact_table", "expire_snapshots"
    }


def test_ingest_tool_is_marked_destructive(monkeypatch):
    monkeypatch.setenv("ZEROETL_MCP_ENABLE_WRITES", "true")
    server = mcp_server.create_server()
    tool = next(tool for tool in asyncio.run(server.list_tools()) if tool.name == "ingest_csv")
    assert tool.annotations.destructive_hint is True


def test_query_tool_returns_json_safe_rows_and_keeps_stdout_clean(monkeypatch, capsys):
    spark = MagicMock()
    monkeypatch.setattr(mcp_server, "get_spark_session", lambda **_: spark)
    row = MagicMock()
    row.asDict.return_value = {"at": datetime(2024, 1, 2, 3, 4, 5), "count": 1}
    def query(config, *, spark):
        print("legacy diagnostic")
        return OperationResult("query", config["table_name"], rows=[row])
    monkeypatch.setattr(mcp_server, "query_iceberg_table", query)
    server = mcp_server.create_server()

    async def call():
        async with server._lowlevel_server.lifespan(server._lowlevel_server):
            return await server.call_tool("query_table", {"table_name": "mycatalog.db.users"})

    result = asyncio.run(call())
    assert result.structured_content == {"result": [{"at": "2024-01-02T03:04:05", "count": 1}]}
    assert capsys.readouterr().out == ""
    spark.stop.assert_called_once()


def test_client_round_trip_uses_one_session(monkeypatch):
    spark = MagicMock()
    created = []
    monkeypatch.setattr(mcp_server, "get_spark_session", lambda **_: created.append(spark) or spark)
    monkeypatch.setattr(mcp_server, "query_iceberg_table", lambda config, *, spark: OperationResult("query", config["table_name"], rows=[{"id": "1"}]))
    server = mcp_server.create_server()

    async def round_trip():
        async with create_client_server_memory_streams() as (client_streams, server_streams):
            async with anyio.create_task_group() as tasks:
                tasks.start_soon(server._lowlevel_server.run, *server_streams,
                                 server._lowlevel_server.create_initialization_options())
                async with ClientSession(*client_streams) as client:
                    await client.initialize()
                    assert [tool.name for tool in (await client.list_tools()).tools] == ["query_table"]
                    for _ in range(2):
                        result = await client.call_tool("query_table", {"table_name": "mycatalog.db.users"})
                        assert result.structured_content == {"result": [{"id": "1"}]}
                tasks.cancel_scope.cancel()

    asyncio.run(round_trip())
    assert created == [spark]
    spark.stop.assert_called_once()
