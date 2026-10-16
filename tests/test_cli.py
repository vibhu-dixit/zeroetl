import importlib.util
import json
import sys
import types
from pathlib import Path

import pytest


@pytest.fixture
def cli(monkeypatch):
    package = types.ModuleType("zeroetl")
    package.__path__ = [str(Path(__file__).parents[1] / "zeroetl")]
    monkeypatch.setitem(sys.modules, "zeroetl", package)
    spec = importlib.util.spec_from_file_location("zeroetl.cli", Path(package.__path__[0]) / "cli.py")
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, "zeroetl.cli", module)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def config_file(tmp_path):
    path = tmp_path / "job.json"
    path.write_text(json.dumps({
        "spark": {"warehouse_path": str(tmp_path / "warehouse"), "spark_configs": {"spark.master": "local[1]"}},
        "table": {"table_name": "mycatalog.db.people", "schema_sql": "id STRING, ts TIMESTAMP", "partition_spec": "days(ts)", "location": str(tmp_path / "people")},
        "ingestion": {"input_data": "input.csv", "source_type": "csv", "schema": [
            {"name": "id", "type": "string", "nullable": False},
            {"name": "ts", "type": "timestamp", "nullable": False},
        ], "primary_key_col": "id", "timestamp_col": "ts", "mode": "append"},
    }), encoding="utf-8")
    return path


def test_dry_run_prints_plan_without_loading_spark(cli, config_file, capsys, monkeypatch):
    def no_spark(name, *args, **kwargs):
        if name.startswith("zeroetl.spark") or name.startswith("pyspark"):
            raise AssertionError("dry run loaded Spark")
        return original_import(name, *args, **kwargs)

    import builtins
    original_import = builtins.__import__
    monkeypatch.setattr(builtins, "__import__", no_spark)
    assert cli.main(["--config", str(config_file), "--output", "json", "--dry-run", "ingest"]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["operation"] == "ingest"
    assert plan["status"] == "dry_run"
    assert plan["plan"]["ingestion"]["mode"] == "append"


@pytest.mark.parametrize("command", ["check", "create-table", "ingest", "query", "compact", "expire-snapshots"])
def test_each_command_has_a_dry_run_plan(cli, config_file, command, capsys):
    assert cli.main(["--config", str(config_file), "--output", "json", "--dry-run", command]) == 0
    assert json.loads(capsys.readouterr().out)["operation"] == command


def test_missing_config_returns_structured_error(cli, capsys, tmp_path):
    assert cli.main(["--config", str(tmp_path / "missing.json"), "--output", "json", "check"]) != 0
    assert json.loads(capsys.readouterr().err)["status"] == "error"


def test_bad_schema_fails_before_spark(cli, config_file, capsys):
    data = json.loads(config_file.read_text())
    data["ingestion"]["schema"][0]["type"] = "unknown"
    config_file.write_text(json.dumps(data))
    assert cli.main(["--config", str(config_file), "--output", "json", "--dry-run", "ingest"]) != 0
    assert "unknown" in json.loads(capsys.readouterr().err)["message"]


def test_cli_output_overrides_config(cli, config_file, capsys):
    data = json.loads(config_file.read_text())
    data["output"] = "text"
    config_file.write_text(json.dumps(data))
    assert cli.main(["--config", str(config_file), "--output", "json", "--dry-run", "query"]) == 0
    assert json.loads(capsys.readouterr().out)["operation"] == "query"


def test_dry_run_redacts_spark_secrets(cli, config_file, capsys):
    data = json.loads(config_file.read_text())
    data["spark"]["spark_configs"]["spark.hadoop.fs.s3a.secret.key"] = "do-not-print"
    config_file.write_text(json.dumps(data))
    assert cli.main(["--config", str(config_file), "--output", "json", "--dry-run", "query"]) == 0
    output = capsys.readouterr().out
    assert "do-not-print" not in output
    assert "<redacted>" in output


def test_query_dispatch_uses_one_session_and_stops_it(cli, config_file, capsys, monkeypatch):
    calls = []
    spark = types.SimpleNamespace(stop=lambda: calls.append("stop"))
    spark_module = types.ModuleType("zeroetl.spark")
    spark_module.get_spark_session = lambda **kwargs: spark
    management = types.ModuleType("zeroetl.table_management")
    management.query_iceberg_table = lambda config, *, spark: types.SimpleNamespace(to_dict=lambda: {
        "operation": "query", "table_name": config["table_name"], "status": "success", "rows": [{"id": "1"}]
    })
    monkeypatch.setitem(sys.modules, "zeroetl.spark", spark_module)
    monkeypatch.setitem(sys.modules, "zeroetl.table_management", management)
    assert cli.main(["--config", str(config_file), "--output", "json", "query"]) == 0
    assert json.loads(capsys.readouterr().out)["rows"] == [{"id": "1"}]
    assert calls == ["stop"]


def test_operation_failure_returns_error_and_stops_spark(cli, config_file, capsys, monkeypatch):
    calls = []
    spark_module = types.ModuleType("zeroetl.spark")
    spark_module.get_spark_session = lambda **kwargs: types.SimpleNamespace(stop=lambda: calls.append("stop"))
    management = types.ModuleType("zeroetl.table_management")
    def fail(config, *, spark):
        raise RuntimeError("catalog unavailable")
    management.compact_iceberg_table = fail
    monkeypatch.setitem(sys.modules, "zeroetl.spark", spark_module)
    monkeypatch.setitem(sys.modules, "zeroetl.table_management", management)
    assert cli.main(["--config", str(config_file), "--output", "json", "compact"]) != 0
    assert "catalog unavailable" in json.loads(capsys.readouterr().err)["message"]
    assert calls == ["stop"]


def test_check_runs_read_only_query(cli, config_file, capsys, monkeypatch):
    calls = []
    spark = types.SimpleNamespace(sql=lambda statement: calls.append(statement) or types.SimpleNamespace(collect=lambda: []), stop=lambda: calls.append("stop"))
    spark_module = types.ModuleType("zeroetl.spark")
    spark_module.get_spark_session = lambda **kwargs: spark
    result_module = types.ModuleType("zeroetl.results")
    result_module.OperationResult = lambda **kwargs: types.SimpleNamespace(to_dict=lambda: {**kwargs, "status": "success"})
    monkeypatch.setitem(sys.modules, "zeroetl.spark", spark_module)
    monkeypatch.setitem(sys.modules, "zeroetl.results", result_module)
    assert cli.main(["--config", str(config_file), "--output", "json", "check"]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "success"
    assert calls == ["SELECT 1", "SHOW NAMESPACES IN mycatalog", "stop"]


def test_spark_error_is_structured(cli, config_file, capsys, monkeypatch):
    spark_module = types.ModuleType("zeroetl.spark")
    def fail(**kwargs):
        raise Exception("JVM failed")
    spark_module.get_spark_session = fail
    monkeypatch.setitem(sys.modules, "zeroetl.spark", spark_module)
    assert cli.main(["--config", str(config_file), "--output", "json", "query"]) == 1
    assert json.loads(capsys.readouterr().err)["message"] == "JVM failed"
