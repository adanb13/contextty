from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
from pathlib import Path

from contextty.services import query_context
from contextty.storage import LocalStore


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "benchmark_contextty_accuracy.py"
SPEC = importlib.util.spec_from_file_location("benchmark_contextty_accuracy", SCRIPT_PATH)
assert SPEC and SPEC.loader
benchmark = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = benchmark
SPEC.loader.exec_module(benchmark)


def test_parse_jsonl_metrics_from_token_count_and_task_complete(tmp_path) -> None:
    jsonl = tmp_path / "codex.jsonl"
    jsonl.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "type": "token_count",
                        "info": {
                            "total_token_usage": {
                                "input_tokens": 100,
                                "output_tokens": 20,
                                "reasoning_output_tokens": 7,
                                "total_tokens": 127,
                            }
                        },
                    }
                ),
                json.dumps({"type": "task_complete", "duration_ms": 4321, "time_to_first_token_ms": 210}),
            ]
        ),
        encoding="utf-8",
    )

    events = benchmark.parse_codex_jsonl(jsonl)
    metrics = benchmark.extract_metrics(events, wall_clock_ms=5000)

    assert metrics.total_tokens == 127
    assert metrics.input_tokens == 100
    assert metrics.output_tokens == 20
    assert metrics.reasoning_tokens == 7
    assert metrics.duration_ms == 4321
    assert metrics.time_to_first_token_ms == 210
    assert metrics.wall_clock_ms == 5000


def test_parse_jsonl_metrics_from_turn_completed_usage() -> None:
    metrics = benchmark.extract_metrics(
        [
            {
                "type": "turn.completed",
                "usage": {
                    "input_tokens": 50,
                    "output_tokens": 10,
                    "reasoning_output_tokens": 5,
                },
            }
        ]
    )

    assert metrics.input_tokens == 50
    assert metrics.output_tokens == 10
    assert metrics.reasoning_tokens == 5
    assert metrics.total_tokens == 65


def test_output_schema_has_bounded_answer_string() -> None:
    answer_schema = benchmark.ANSWER_SCHEMA["properties"]["answers"]["items"]["properties"]["answer"]

    assert answer_schema == {"type": "string"}


def test_score_payload_marks_correct_incorrect_and_insufficient() -> None:
    truth = {
        "Q01": {
            "id": "Q01",
            "category": "schema_profile",
            "question": "How many tables?",
            "expected_kind": "number",
            "expected": 9,
        }
    }

    correct = benchmark.score_payload(
        {"answers": [{"id": "Q01", "answer": "There are 9 user tables.", "source_used": "direct_db", "confidence": 1}]},
        truth,
    )
    incorrect = benchmark.score_payload(
        {"answers": [{"id": "Q01", "answer": "There are 8 user tables.", "source_used": "direct_db", "confidence": 1}]},
        truth,
    )
    insufficient = benchmark.score_payload(
        {
            "answers": [
                {
                    "id": "Q01",
                    "answer": "INSUFFICIENT_CONTEXT",
                    "source_used": "insufficient_context",
                    "confidence": 0.2,
                }
            ]
        },
        truth,
    )

    assert correct["Q01"].correct
    assert not incorrect["Q01"].correct
    assert not insufficient["Q01"].correct
    assert insufficient["Q01"].insufficient_context


def test_prompt_generation_sets_lane_rules() -> None:
    db_path = Path("/tmp/contextty_test.db")

    direct = benchmark.build_prompt("direct_db", db_path, "contextty-test")
    contextty_only = benchmark.build_prompt("contextty_only", db_path, "contextty-test")
    hybrid = benchmark.build_prompt("contextty_then_db", db_path, "contextty-test")

    assert str(db_path) in direct
    assert "Do not use Contextty MCP tools" in direct
    assert "Use only Contextty MCP tools" in contextty_only
    assert str(db_path) not in contextty_only
    assert "First use Contextty MCP tools" in hybrid
    assert "read-only SQLite fallback" in hybrid
    assert str(db_path) in hybrid


def test_mcp_config_auto_approves_contextty_tools(tmp_path) -> None:
    overrides = benchmark.codex_mcp_config_overrides(tmp_path / "contextty.db")

    assert 'mcp_servers.contextty.default_tools_approval_mode="approve"' in overrides


def test_cleanup_generated_benchmark_artifacts_keeps_latest_only(tmp_path) -> None:
    root = tmp_path / "benchmarks"
    older = root / "20260601-010101"
    latest = root / "20260601-020202"
    baseline = root / "20260601-075747"
    for path in (older, latest, baseline):
        path.mkdir(parents=True)
        (path / "report.md").write_text(path.name, encoding="utf-8")
    manifest = root / ".generated-runs.json"
    manifest.write_text(json.dumps([str(older)]), encoding="utf-8")

    removed = benchmark.cleanup_generated_benchmark_artifacts(latest, keep_latest=1, manifest_path=manifest)

    assert removed == [older]
    assert not older.exists()
    assert latest.exists()
    assert baseline.exists()
    assert json.loads(manifest.read_text(encoding="utf-8")) == [str(latest.resolve())]


def test_benchmark_run_succeeded_requires_all_lanes_ok() -> None:
    ok = benchmark.LaneResult(
        lane="contextty_only",
        returncode=0,
        prompt_path="",
        raw_jsonl_path="",
        final_answer_path="",
        stderr_path="",
        parsed_answer_path="",
        metrics=benchmark.CodexMetrics(),
        trace=benchmark.TraceSummary(False, False, None, None, False, False, True),
        scores={},
    )
    failed = benchmark.LaneResult(
        lane="contextty_then_db",
        returncode=1,
        prompt_path="",
        raw_jsonl_path="",
        final_answer_path="",
        stderr_path="",
        parsed_answer_path="",
        metrics=benchmark.CodexMetrics(),
        trace=benchmark.TraceSummary(False, False, None, None, False, False, False),
        scores={},
        error="usage limit",
    )

    assert benchmark.benchmark_run_succeeded({"ok": ok})
    assert not benchmark.benchmark_run_succeeded({"ok": ok, "failed": failed})


def test_tool_trace_validation_detects_hybrid_order_and_contextty_only_violation(tmp_path) -> None:
    db_path = tmp_path / "contextty_test.db"
    db_path.write_text("", encoding="utf-8")
    mcp_event = {
        "type": "item.started",
        "item": {"type": "mcp_tool_call", "server": "contextty", "tool": "query_context"},
    }
    db_event = {"type": "exec_command", "cmd": f"python3 inspect.py {db_path}"}

    valid_hybrid = benchmark.analyze_tool_trace([mcp_event, db_event], db_path, "contextty_then_db")
    invalid_hybrid = benchmark.analyze_tool_trace([db_event, mcp_event], db_path, "contextty_then_db")
    contextty_only = benchmark.analyze_tool_trace([db_event], db_path, "contextty_only")

    assert valid_hybrid.mcp_used
    assert valid_hybrid.db_accessed
    assert valid_hybrid.mcp_before_db
    assert valid_hybrid.hybrid_order_valid
    assert not invalid_hybrid.hybrid_order_valid
    assert contextty_only.contextty_only_db_violation


def test_setup_contextty_snapshot_registers_sqlite_and_creates_snapshot(tmp_path) -> None:
    db_path = tmp_path / "fixture.sqlite3"
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)")
        conn.execute(
            """
            CREATE TABLE employees (
                id INTEGER PRIMARY KEY,
                department_id INTEGER NOT NULL REFERENCES departments(id),
                email TEXT NOT NULL UNIQUE
            )
            """
        )
        conn.execute("INSERT INTO departments(id, name) VALUES (1, 'Engineering')")
        conn.execute("INSERT INTO employees(id, department_id, email) VALUES (1, 1, 'a@example.com')")

    output_dir = tmp_path / "benchmark"
    stats = benchmark.setup_contextty_snapshot(db_path, output_dir, source_name="local-db", row_limit=100)

    assert stats.nodes > 0
    assert stats.edges > 0
    assert stats.pills > 0
    store = LocalStore(output_dir / "contextty.db")
    source = store.get_source("local-db")
    assert source.connector_type == "sqlite"
    assert source.path == str(db_path.resolve())
    result = query_context(store, "employees department", source_name="local-db")
    assert "employees" in result["context"]
