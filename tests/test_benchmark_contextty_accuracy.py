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

HR_TABLES = {
    "departments",
    "employee_projects",
    "employees",
    "job_titles",
    "office_locations",
    "performance_reviews",
    "projects",
    "salary_history",
    "time_off_requests",
}
COMMERCE_TABLES = {"categories", "customers", "order_items", "orders", "products", "shipments", "support_tickets"}


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


def test_answer_schema_uses_selected_suite_question_count() -> None:
    answer_schema = benchmark.answer_schema_for(benchmark.COMMERCE_QUESTIONS)

    assert answer_schema["properties"]["answers"]["minItems"] == len(benchmark.COMMERCE_QUESTIONS)


def test_default_benchmark_artifact_paths_are_grouped_under_benchmark_root() -> None:
    assert benchmark.BENCHMARK_RUNS_ROOT == benchmark.BENCHMARK_ROOT / "runs"
    assert benchmark.BENCHMARK_DATABASES_ROOT == benchmark.BENCHMARK_ROOT / "databases"
    assert benchmark.GENERATED_RUNS_MANIFEST == benchmark.BENCHMARK_RUNS_ROOT / ".generated-runs.json"
    assert benchmark.default_output_dir().parent == benchmark.BENCHMARK_RUNS_ROOT
    assert all(suite.default_db_path.parent == benchmark.BENCHMARK_DATABASES_ROOT for suite in benchmark.BENCHMARK_SUITES.values())


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


def test_cleanup_generated_benchmark_artifacts_bounds_full_and_partial_runs(tmp_path) -> None:
    root = tmp_path / "benchmarks"
    full_old = root / "20260601-010101"
    full_latest = root / "20260601-020202"
    partial_old = root / "20260601-030303"
    partial_latest = root / "20260601-040404"
    for path in (full_old, full_latest, partial_old, partial_latest):
        path.mkdir(parents=True)
    for path in (full_old, full_latest):
        (path / "report.json").write_text(
            json.dumps({"lanes": {lane: {} for lane in benchmark.VALID_LANES}}),
            encoding="utf-8",
        )
    for path in (partial_old, partial_latest):
        (path / "report.json").write_text(json.dumps({"lanes": {"direct_db": {}}}), encoding="utf-8")
    manifest = root / ".generated-runs.json"
    manifest.write_text(
        json.dumps([str(full_old), str(full_latest), str(partial_old)]),
        encoding="utf-8",
    )

    removed = benchmark.cleanup_generated_benchmark_artifacts(
        partial_latest,
        keep_latest=1,
        keep_latest_partial=1,
        manifest_path=manifest,
    )

    assert set(removed) == {full_old, partial_old}
    assert not full_old.exists()
    assert full_latest.exists()
    assert not partial_old.exists()
    assert partial_latest.exists()
    assert set(json.loads(manifest.read_text(encoding="utf-8"))) == {
        str(full_latest.resolve()),
        str(partial_latest.resolve()),
    }


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
    assert stats.source_db_size_bytes > 0
    assert stats.contextty_db_size_bytes > 0
    store = LocalStore(output_dir / "contextty.db")
    source = store.get_source("local-db")
    assert source.connector_type == "sqlite"
    assert source.path == str(db_path.resolve())
    result = query_context(store, "employees department", source_name="local-db")
    assert "employees" in result["context"]


def test_hr_suite_builds_default_database_fixture(tmp_path) -> None:
    db_path = tmp_path / "contextty_test.db"
    suite = benchmark.BenchmarkSuite(
        name="hr",
        default_db_path=db_path,
        source_name="contextty-test",
        questions=benchmark.QUESTIONS,
        builder=benchmark.create_hr_fixture_db,
    )

    benchmark.ensure_suite_database(suite, suite.default_db_path)

    assert sqlite_user_tables(db_path) == HR_TABLES
    truth = benchmark.compute_ground_truth(db_path, questions=benchmark.QUESTIONS)
    assert truth["Q01"]["expected"] == 9
    assert truth["Q02"]["expected"] == ["employee_projects"]
    assert truth["Q10"]["expected"] == "Luis Martinez"
    assert truth["Q11"]["expected"] == ["Owen Brooks", "Priya Nair", "Samir Patel"]
    assert truth["Q13"]["expected"] == [{"effective_date": "2026-01-01", "salary": 154000}]
    assert truth["Q14"]["expected"] == [
        {"project_name": "Employee Self Service Portal", "total_allocation_percent": 230}
    ]
    assert truth["Q17"]["expected"] == 155500.0


def test_commerce_suite_builds_different_database_and_snapshot_answers(tmp_path) -> None:
    db_path = tmp_path / "commerce.sqlite3"
    benchmark.create_commerce_fixture_db(db_path)

    truth = benchmark.compute_ground_truth(db_path, questions=benchmark.COMMERCE_QUESTIONS)
    assert truth["Q01"]["expected"] == 7
    assert truth["Q08"]["expected"] == "Audio"
    assert truth["Q09"]["expected"] == "Ava Stone"
    assert truth["Q10"]["expected"] == [{"product_name": "Noise Cancelling Headphones", "total_quantity": 4}]

    output_dir = tmp_path / "benchmark"
    stats = benchmark.setup_contextty_snapshot(db_path, output_dir, source_name="commerce-db", row_limit=100)
    assert stats.source_db_size_bytes == db_path.stat().st_size
    assert stats.contextty_db_size_bytes == (output_dir / "contextty.db").stat().st_size

    store = LocalStore(output_dir / "contextty.db")
    result = query_context(
        store,
        """
        Answer all commerce row questions:
        Which category contains Noise Cancelling Headphones?
        Which customer placed order WEB-1003?
        Which product has the highest total quantity sold?
        What is the average order_items.quantity by product category?
        """,
        source_name="commerce-db",
        budget=12000,
    )
    rendered = json.dumps(result, sort_keys=True)
    assert result["answerability"]["status"] == "answered_by_snapshot"
    assert "Audio" in rendered
    assert "Ava Stone" in rendered
    assert "Noise Cancelling Headphones" in rendered


def test_finance_suite_builds_distinct_database_and_snapshot_answers(tmp_path) -> None:
    db_path = tmp_path / "finance.sqlite3"
    benchmark.create_finance_fixture_db(db_path)

    tables = sqlite_user_tables(db_path)
    assert db_path.exists()
    assert tables == {
        "account_holders",
        "accounts",
        "branches",
        "categories",
        "reimbursements",
        "transactions",
        "vendors",
    }
    assert tables != HR_TABLES
    assert tables != COMMERCE_TABLES

    truth = benchmark.compute_ground_truth(db_path, questions=benchmark.FINANCE_QUESTIONS)
    assert {question_id: item["expected"] for question_id, item in truth.items()} == {
        "Q01": 7,
        "Q02": ["transactions"],
        "Q03": ["id"],
        "Q04": ["account_id", "category_id", "vendor_id"],
        "Q05": [
            "idx_reimbursements_status",
            "idx_reimbursements_vendor",
            "idx_transactions_account",
            "idx_transactions_status",
        ],
        "Q06": {"disputed": 1, "pending": 2, "posted": 5},
        "Q07": {"approved": 2, "rejected": 1, "submitted": 1},
        "Q08": [{"account_holder": "Ethan Brooks", "branch_name": "Lakeview"}],
        "Q09": [{"vendor_name": "Coastline Travel", "category_name": "Travel"}],
        "Q10": [{"vendor_name": "Coastline Travel", "total_amount_usd": 2295.75}],
        "Q11": [
            {"transaction_ref": "TXN-9001", "status": "posted"},
            {"transaction_ref": "TXN-9002", "status": "pending"},
            {"transaction_ref": "TXN-9007", "status": "posted"},
        ],
        "Q12": {"Harbor": 270.13, "Lakeview": 568.0, "North Loop": 458.88},
    }

    output_dir = tmp_path / "finance-benchmark"
    stats = benchmark.setup_contextty_snapshot(db_path, output_dir, source_name="finance-db", row_limit=100)
    assert stats.source_db_size_bytes == db_path.stat().st_size
    assert stats.contextty_db_size_bytes == (output_dir / "contextty.db").stat().st_size
    assert stats.facts > 0

    store = LocalStore(output_dir / "contextty.db")
    result = query_context(
        store,
        """
        Answer all finance row questions:
        Which vendor has the highest total transaction amount?
        Which transactions belong to account ACC-1001?
        What is the average transaction amount by branch?
        """,
        source_name="finance-db",
        budget=12000,
    )
    rendered = json.dumps(result, sort_keys=True)
    assert result["answerability"]["status"] == "answered_by_snapshot"
    assert "Coastline Travel" in rendered
    assert "2295.75" in rendered
    assert "TXN-9001" in rendered
    assert "North Loop" in rendered
    assert "458.88" in rendered


def test_education_suite_builds_distinct_database_and_snapshot_answers(tmp_path) -> None:
    db_path = tmp_path / "education.sqlite3"
    benchmark.create_education_fixture_db(db_path)

    tables = sqlite_user_tables(db_path)
    assert db_path.exists()
    assert tables == {
        "campuses",
        "courses",
        "departments",
        "enrollments",
        "instructors",
        "sections",
        "students",
    }
    assert tables != HR_TABLES
    assert tables != COMMERCE_TABLES

    truth = benchmark.compute_ground_truth(db_path, questions=benchmark.EDUCATION_QUESTIONS)
    assert {question_id: item["expected"] for question_id, item in truth.items()} == {
        "Q01": 7,
        "Q02": ["enrollments"],
        "Q03": ["student_id", "section_id"],
        "Q04": ["course_id", "instructor_id"],
        "Q05": ["idx_enrollments_section", "idx_enrollments_status"],
        "Q06": {"active": 4, "inactive": 1},
        "Q07": {"cancelled": 1, "open": 3, "waitlist": 1},
        "Q08": "Applied Analytics",
        "Q09": "Grace Lin",
        "Q10": [{"section_code": "SEC-2025-FALL-DATA310-A", "total_grade_points": 11.4}],
        "Q11": ["Sophia Chen", "Olivia Hart", "Noah Singh"],
        "Q12": {
            "Climate Policy": 3.5,
            "Data Ethics": 3.6,
            "Narrative Design": 2.95,
            "Predictive Modeling": 3.8,
        },
    }

    output_dir = tmp_path / "education-benchmark"
    stats = benchmark.setup_contextty_snapshot(db_path, output_dir, source_name="education-db", row_limit=100)
    assert stats.source_db_size_bytes == db_path.stat().st_size
    assert stats.contextty_db_size_bytes == (output_dir / "contextty.db").stat().st_size
    assert stats.facts > 0

    store = LocalStore(output_dir / "contextty.db")
    result = query_context(
        store,
        """
        Answer all education row questions:
        Which department offers Data Ethics?
        Which instructor teaches section SEC-2025-FALL-DATA201-A?
        Which section has the highest total enrollment grade points?
        Which students are enrolled in section SEC-2025-FALL-DATA201-A?
        What is the average enrollment grade points by course?
        """,
        source_name="education-db",
        budget=12000,
    )
    rendered = json.dumps(result, sort_keys=True)
    assert result["answerability"]["status"] == "answered_by_snapshot"
    assert "Applied Analytics" in rendered
    assert "Grace Lin" in rendered
    assert "SEC-2025-FALL-DATA310-A" in rendered
    assert "11.4" in rendered
    assert "Sophia Chen" in rendered
    assert "Data Ethics" in rendered
    assert "3.6" in rendered


def sqlite_user_tables(db_path: Path) -> set[str]:
    with sqlite3.connect(db_path) as conn:
        return {
            row[0]
            for row in conn.execute(
                """
                SELECT name
                FROM sqlite_schema
                WHERE type = 'table'
                  AND name NOT LIKE 'sqlite_%'
                """
            )
        }
