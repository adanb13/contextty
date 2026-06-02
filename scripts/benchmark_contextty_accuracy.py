#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Literal
from urllib.parse import quote

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
BENCHMARK_ROOT = REPO_ROOT / ".contextty" / "benchmarks"
GENERATED_RUNS_MANIFEST = BENCHMARK_ROOT / ".generated-runs.json"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from contextty.models import SnapshotOptions  # noqa: E402
from contextty.services import refresh_snapshot  # noqa: E402
from contextty.storage import LocalStore  # noqa: E402

LaneName = Literal["direct_db", "contextty_only", "contextty_then_db"]
QuestionCategory = Literal["schema_profile", "row_level"]
ExpectedKind = Literal["number", "value", "list", "mapping", "rows"]

VALID_LANES: tuple[LaneName, ...] = ("direct_db", "contextty_only", "contextty_then_db")
STRATEGY_ORDER: tuple[str, ...] = ("compact_v2", "row_cards", "row_cards_vector", "answer_pack")
ACTIVE_STRATEGY = "answer_pack"
CONTEXTTY_TOOL_NAMES = {
    "detect_sources",
    "add_source",
    "list_sources",
    "inspect_source",
    "refresh_snapshot",
    "query_context",
    "get_node",
    "get_neighbors",
    "find_path",
}


@dataclass(frozen=True)
class QuestionSpec:
    id: str
    category: QuestionCategory
    question: str
    sql: str
    expected_kind: ExpectedKind


@dataclass
class SnapshotStats:
    store_path: str
    source_name: str
    db_path: str
    profile_mode: str
    row_limit: int
    nodes: int
    edges: int
    pills: int
    facts: int
    snapshot_run_id: int | None


@dataclass
class CodexMetrics:
    total_tokens: int | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    reasoning_tokens: int | None = None
    duration_ms: int | None = None
    time_to_first_token_ms: int | None = None
    wall_clock_ms: int | None = None


@dataclass
class TraceSummary:
    mcp_used: bool
    db_accessed: bool
    first_mcp_event_index: int | None
    first_db_event_index: int | None
    mcp_before_db: bool
    contextty_only_db_violation: bool
    hybrid_order_valid: bool


@dataclass
class ScoreResult:
    id: str
    correct: bool
    insufficient_context: bool
    source_used: str
    confidence: float | None
    reason: str
    expected: Any
    actual: Any


@dataclass
class LaneResult:
    lane: str
    returncode: int | None
    prompt_path: str
    raw_jsonl_path: str
    final_answer_path: str
    stderr_path: str
    parsed_answer_path: str
    metrics: CodexMetrics
    trace: TraceSummary
    scores: dict[str, ScoreResult]
    error: str | None = None


QUESTIONS: tuple[QuestionSpec, ...] = (
    QuestionSpec(
        id="Q01",
        category="schema_profile",
        question="How many user tables are in the SQLite database, excluding sqlite internal tables?",
        sql="""
            SELECT count(*) AS value
            FROM sqlite_schema
            WHERE type = 'table'
              AND name NOT LIKE 'sqlite_%'
        """,
        expected_kind="number",
    ),
    QuestionSpec(
        id="Q02",
        category="schema_profile",
        question="Which table stores employee-to-project assignments and allocation percentages?",
        sql="""
            SELECT name
            FROM sqlite_schema
            WHERE type = 'table'
              AND sql LIKE '%allocation_percent%'
            ORDER BY name
        """,
        expected_kind="list",
    ),
    QuestionSpec(
        id="Q03",
        category="schema_profile",
        question="What columns make up the composite primary key for employee_projects?",
        sql="""
            SELECT name
            FROM pragma_table_info('employee_projects')
            WHERE pk > 0
            ORDER BY pk
        """,
        expected_kind="list",
    ),
    QuestionSpec(
        id="Q04",
        category="schema_profile",
        question="Which columns in time_off_requests reference employees?",
        sql="""
            SELECT "from" AS column_name
            FROM pragma_foreign_key_list('time_off_requests')
            WHERE "table" = 'employees'
            ORDER BY column_name
        """,
        expected_kind="list",
    ),
    QuestionSpec(
        id="Q05",
        category="schema_profile",
        question="Which indexes are defined on employees for department_id and manager_id lookups?",
        sql="""
            SELECT DISTINCT il.name
            FROM pragma_index_list('employees') AS il
            JOIN pragma_index_info(il.name) AS ii
            WHERE ii.name IN ('department_id', 'manager_id')
            ORDER BY il.name
        """,
        expected_kind="list",
    ),
    QuestionSpec(
        id="Q06",
        category="schema_profile",
        question="What are the projects.status values and counts?",
        sql="""
            SELECT status, count(*) AS count
            FROM projects
            GROUP BY status
            ORDER BY status
        """,
        expected_kind="mapping",
    ),
    QuestionSpec(
        id="Q07",
        category="schema_profile",
        question="What are the employees.employment_status values and counts?",
        sql="""
            SELECT employment_status, count(*) AS count
            FROM employees
            GROUP BY employment_status
            ORDER BY employment_status
        """,
        expected_kind="mapping",
    ),
    QuestionSpec(
        id="Q08",
        category="schema_profile",
        question="Which table stores job title salary bands, and what are the salary band columns?",
        sql="""
            SELECT 'job_titles' AS table_name, name AS column_name
            FROM pragma_table_info('job_titles')
            WHERE name IN ('salary_band_min', 'salary_band_max')
            ORDER BY name
        """,
        expected_kind="rows",
    ),
    QuestionSpec(
        id="Q09",
        category="schema_profile",
        question="How many rows are in each user table?",
        sql="""
            SELECT 'departments' AS table_name, count(*) AS count FROM departments
            UNION ALL SELECT 'employee_projects', count(*) FROM employee_projects
            UNION ALL SELECT 'employees', count(*) FROM employees
            UNION ALL SELECT 'job_titles', count(*) FROM job_titles
            UNION ALL SELECT 'office_locations', count(*) FROM office_locations
            UNION ALL SELECT 'performance_reviews', count(*) FROM performance_reviews
            UNION ALL SELECT 'projects', count(*) FROM projects
            UNION ALL SELECT 'salary_history', count(*) FROM salary_history
            UNION ALL SELECT 'time_off_requests', count(*) FROM time_off_requests
            ORDER BY table_name
        """,
        expected_kind="mapping",
    ),
    QuestionSpec(
        id="Q10",
        category="row_level",
        question="Who is Priya Nair's manager?",
        sql="""
            SELECT manager.first_name || ' ' || manager.last_name AS manager_name
            FROM employees AS employee
            JOIN employees AS manager ON manager.id = employee.manager_id
            WHERE employee.first_name = 'Priya'
              AND employee.last_name = 'Nair'
        """,
        expected_kind="value",
    ),
    QuestionSpec(
        id="Q11",
        category="row_level",
        question="Which employees report directly to Luis Martinez?",
        sql="""
            SELECT employee.first_name || ' ' || employee.last_name AS employee_name
            FROM employees AS employee
            JOIN employees AS manager ON manager.id = employee.manager_id
            WHERE manager.first_name = 'Luis'
              AND manager.last_name = 'Martinez'
            ORDER BY employee.last_name, employee.first_name
        """,
        expected_kind="list",
    ),
    QuestionSpec(
        id="Q12",
        category="row_level",
        question="Which active employee has the earliest hire_date, and what is that date?",
        sql="""
            SELECT first_name || ' ' || last_name AS employee_name, hire_date
            FROM employees
            WHERE employment_status = 'active'
            ORDER BY hire_date, id
            LIMIT 1
        """,
        expected_kind="rows",
    ),
    QuestionSpec(
        id="Q13",
        category="row_level",
        question="What is Samir Patel's latest salary and effective_date?",
        sql="""
            SELECT salary_history.salary,
                   salary_history.effective_date
            FROM salary_history
            JOIN employees ON employees.id = salary_history.employee_id
            WHERE employees.first_name = 'Samir'
              AND employees.last_name = 'Patel'
            ORDER BY salary_history.effective_date DESC
            LIMIT 1
        """,
        expected_kind="rows",
    ),
    QuestionSpec(
        id="Q14",
        category="row_level",
        question="Which project has the highest total allocation_percent across assignments, and what is the total?",
        sql="""
            SELECT projects.name AS project_name,
                   sum(employee_projects.allocation_percent) AS total_allocation_percent
            FROM projects
            JOIN employee_projects ON employee_projects.project_id = projects.id
            GROUP BY projects.id, projects.name
            ORDER BY total_allocation_percent DESC, projects.name
            LIMIT 1
        """,
        expected_kind="rows",
    ),
    QuestionSpec(
        id="Q15",
        category="row_level",
        question="Which employees are assigned to Manager Insights Dashboard, with their role and allocation_percent?",
        sql="""
            SELECT employees.first_name || ' ' || employees.last_name AS employee_name,
                   employee_projects.role,
                   employee_projects.allocation_percent
            FROM employee_projects
            JOIN employees ON employees.id = employee_projects.employee_id
            JOIN projects ON projects.id = employee_projects.project_id
            WHERE projects.name = 'Manager Insights Dashboard'
            ORDER BY employees.last_name, employees.first_name
        """,
        expected_kind="rows",
    ),
    QuestionSpec(
        id="Q16",
        category="row_level",
        question="Which department owns Payroll Modernization, and what is that department's cost_center?",
        sql="""
            SELECT departments.name AS department_name, departments.cost_center
            FROM projects
            JOIN departments ON departments.id = projects.department_id
            WHERE projects.name = 'Payroll Modernization'
        """,
        expected_kind="rows",
    ),
    QuestionSpec(
        id="Q17",
        category="row_level",
        question="What is the average latest salary for Engineering employees?",
        sql="""
            WITH latest_salary AS (
                SELECT employee_id, max(effective_date) AS effective_date
                FROM salary_history
                GROUP BY employee_id
            )
            SELECT avg(salary_history.salary) AS average_latest_salary
            FROM salary_history
            JOIN latest_salary
              ON latest_salary.employee_id = salary_history.employee_id
             AND latest_salary.effective_date = salary_history.effective_date
            JOIN employees ON employees.id = salary_history.employee_id
            JOIN departments ON departments.id = employees.department_id
            WHERE departments.name = 'Engineering'
        """,
        expected_kind="number",
    ),
    QuestionSpec(
        id="Q18",
        category="row_level",
        question="What is the average 2025-H2 performance review rating by department?",
        sql="""
            SELECT departments.name AS department_name,
                   round(avg(performance_reviews.rating), 2) AS average_rating
            FROM performance_reviews
            JOIN employees ON employees.id = performance_reviews.employee_id
            JOIN departments ON departments.id = employees.department_id
            WHERE performance_reviews.review_period = '2025-H2'
            GROUP BY departments.name
            ORDER BY departments.name
        """,
        expected_kind="mapping",
    ),
)


ANSWER_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["answers"],
    "properties": {
        "answers": {
            "type": "array",
            "minItems": len(QUESTIONS),
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["id", "answer", "source_used", "confidence"],
                "properties": {
                    "id": {"type": "string"},
                    "answer": {"type": "string"},
                    "source_used": {"type": "string"},
                    "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                },
            },
        }
    },
}


def readonly_sqlite_connection(db_path: Path | str, timeout_seconds: float = 5.0) -> sqlite3.Connection:
    resolved = Path(db_path).expanduser().resolve()
    uri = f"file:{quote(str(resolved), safe='/:')}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=timeout_seconds)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA query_only = ON")
    return conn


def compute_ground_truth(
    db_path: Path | str,
    questions: tuple[QuestionSpec, ...] = QUESTIONS,
) -> dict[str, dict[str, Any]]:
    truth: dict[str, dict[str, Any]] = {}
    with readonly_sqlite_connection(db_path) as conn:
        for question in questions:
            rows = [dict(row) for row in conn.execute(question.sql)]
            truth[question.id] = {
                "id": question.id,
                "category": question.category,
                "question": question.question,
                "expected_kind": question.expected_kind,
                "expected": expected_from_rows(rows, question.expected_kind),
                "rows": rows,
            }
    return truth


def expected_from_rows(rows: list[dict[str, Any]], expected_kind: ExpectedKind) -> Any:
    if expected_kind == "number":
        return first_row_value(rows)
    if expected_kind == "value":
        return first_row_value(rows)
    if expected_kind == "list":
        return [first_value(row) for row in rows]
    if expected_kind == "mapping":
        mapping: dict[str, Any] = {}
        for row in rows:
            values = list(row.values())
            if len(values) >= 2:
                mapping[str(values[0])] = values[1]
        return mapping
    if expected_kind == "rows":
        return rows
    raise ValueError(f"unsupported expected kind: {expected_kind}")


def first_row_value(rows: list[dict[str, Any]]) -> Any:
    if not rows:
        return None
    return first_value(rows[0])


def first_value(row: dict[str, Any]) -> Any:
    return next(iter(row.values()))


def setup_contextty_snapshot(
    db_path: Path | str,
    output_dir: Path | str,
    source_name: str = "contextty-test",
    row_limit: int = 1000,
    timeout_seconds: float = 10.0,
) -> SnapshotStats:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    store_path = output_path / "contextty.db"
    store = LocalStore(store_path)
    store.add_source(source_name, "sqlite", path=str(Path(db_path).expanduser().resolve()))
    snapshot = refresh_snapshot(
        store,
        source_name,
        SnapshotOptions(profile_mode="deep", row_limit=row_limit, timeout_seconds=timeout_seconds),
    )
    run = snapshot.get("run") or {}
    return SnapshotStats(
        store_path=str(store_path),
        source_name=source_name,
        db_path=str(Path(db_path).expanduser().resolve()),
        profile_mode="deep",
        row_limit=row_limit,
        nodes=int(snapshot.get("nodes") or 0),
        edges=int(snapshot.get("edges") or 0),
        pills=int(snapshot.get("pills") or 0),
        facts=int(snapshot.get("facts") or 0),
        snapshot_run_id=run.get("id"),
    )


def build_prompt(lane: LaneName, db_path: Path | str, source_name: str, questions: tuple[QuestionSpec, ...] = QUESTIONS) -> str:
    rendered_questions = "\n".join(f"- {question.id}: {question.question}" for question in questions)
    common = f"""
You are answering an accuracy benchmark. Answer every question exactly and concisely.

Return only JSON matching this shape:
{{
  "answers": [
    {{"id": "Q01", "answer": "...", "source_used": "...", "confidence": 0.0}}
  ]
}}

Use one answer object per question id. Put the answer in a concise string; for multi-item answers, use a semicolon-separated string.
If the permitted evidence is insufficient, set answer to "INSUFFICIENT_CONTEXT" and source_used to "insufficient_context".

Questions:
{rendered_questions}
""".strip()

    if lane == "direct_db":
        return f"""
{common}

Lane rules:
- Use read-only SQLite access to the live database at {Path(db_path).expanduser().resolve()}.
- Do not use Contextty MCP tools.
- Do not modify files or database state.
- Set source_used to "direct_db" for factual answers.
""".strip()

    if lane == "contextty_only":
        return f"""
{common}

Lane rules:
- Use only Contextty MCP tools for source "{source_name}".
- Start with one `query_context` call that asks for an answer pack covering all question topics; use a budget around 12000.
- Answer from `answer_candidates` and `context` when `answerability.status` is "answered_by_snapshot".
- Do not access the live SQLite database, do not run SQL against it, and do not inspect database files directly.
- If `query_context` returns `answerability.status` as "needs_db_fallback" for a question, answer "INSUFFICIENT_CONTEXT".
- Set source_used to "contextty" for factual answers from MCP.
""".strip()

    if lane == "contextty_then_db":
        return f"""
{common}

Lane rules:
- First use Contextty MCP tools for source "{source_name}" on every question.
- Start with one `query_context` call that asks for an answer pack covering all question topics; use a budget around 12000.
- Use read-only SQLite fallback at {Path(db_path).expanduser().resolve()} only when `query_context` returns `answerability.status` as "needs_db_fallback" for the needed evidence.
- Do not modify files or database state.
- Set source_used to "contextty" when MCP alone answered, "db_fallback" when the live DB was needed, or "both" when both contributed.
""".strip()

    raise ValueError(f"unsupported lane: {lane}")


def parse_codex_jsonl(path: Path | str) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    jsonl_path = Path(path)
    if not jsonl_path.exists():
        return events
    for line in jsonl_path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            events.append(parsed)
    return events


def extract_metrics(events: list[dict[str, Any]], wall_clock_ms: int | None = None) -> CodexMetrics:
    metrics = CodexMetrics(wall_clock_ms=wall_clock_ms)
    for event in events:
        usage = latest_dict_for_key(event, "total_token_usage")
        if not usage and isinstance(event.get("usage"), dict):
            usage = event["usage"]
        if usage:
            metrics.input_tokens = int_or_none(first_present(usage, "input_tokens", "prompt_tokens", "input"))
            metrics.output_tokens = int_or_none(first_present(usage, "output_tokens", "completion_tokens", "output"))
            metrics.reasoning_tokens = int_or_none(
                first_present(usage, "reasoning_output_tokens", "reasoning_tokens", "reasoning")
            )
            total = first_present(usage, "total_tokens", "total")
            if total is None:
                total = sum(
                    value
                    for value in (metrics.input_tokens, metrics.output_tokens, metrics.reasoning_tokens)
                    if value is not None
                )
            metrics.total_tokens = int_or_none(total)

        event_type = str(event.get("type") or event.get("event") or "").lower()
        if event_type == "task_complete" or event_type.endswith(".task_complete"):
            metrics.duration_ms = int_or_none(first_number_for_key(event, "duration_ms"))
            metrics.time_to_first_token_ms = int_or_none(first_number_for_key(event, "time_to_first_token_ms"))
    return metrics


def latest_dict_for_key(value: Any, target_key: str) -> dict[str, Any] | None:
    found: dict[str, Any] | None = None

    def walk(current: Any) -> None:
        nonlocal found
        if isinstance(current, dict):
            for key, child in current.items():
                if key == target_key and isinstance(child, dict):
                    found = child
                walk(child)
        elif isinstance(current, list):
            for child in current:
                walk(child)

    walk(value)
    return found


def first_number_for_key(value: Any, target_key: str) -> int | float | None:
    found: int | float | None = None

    def walk(current: Any) -> None:
        nonlocal found
        if found is not None:
            return
        if isinstance(current, dict):
            for key, child in current.items():
                if key == target_key and isinstance(child, (int, float)):
                    found = child
                    return
                walk(child)
        elif isinstance(current, list):
            for child in current:
                walk(child)

    walk(value)
    return found


def first_present(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping:
            return mapping[key]
    return None


def int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def analyze_tool_trace(events: list[dict[str, Any]], db_path: Path | str, lane: str) -> TraceSummary:
    first_mcp: int | None = None
    first_db: int | None = None
    db_markers = db_access_markers(db_path)

    for index, event in enumerate(events):
        if first_mcp is None and is_mcp_tool_event(event):
            first_mcp = index
        if first_db is None and event_has_db_command(event, db_markers):
            first_db = index

    mcp_used = first_mcp is not None
    db_accessed = first_db is not None
    mcp_before_db = first_mcp is not None and first_db is not None and first_mcp < first_db
    contextty_only_db_violation = lane == "contextty_only" and db_accessed
    hybrid_order_valid = True
    if lane == "contextty_then_db":
        hybrid_order_valid = mcp_used and (not db_accessed or mcp_before_db)
    return TraceSummary(
        mcp_used=mcp_used,
        db_accessed=db_accessed,
        first_mcp_event_index=first_mcp,
        first_db_event_index=first_db,
        mcp_before_db=mcp_before_db,
        contextty_only_db_violation=contextty_only_db_violation,
        hybrid_order_valid=hybrid_order_valid,
    )


def db_access_markers(db_path: Path | str) -> set[str]:
    resolved = Path(db_path).expanduser().resolve()
    markers = {str(resolved), str(Path(db_path)), resolved.name}
    try:
        markers.add(str(resolved.relative_to(REPO_ROOT)))
    except ValueError:
        pass
    return {marker for marker in markers if marker}


def is_mcp_tool_event(event: dict[str, Any]) -> bool:
    event_type = str(event.get("type") or event.get("event") or "").lower()
    item_type = ""
    item = event.get("item")
    if isinstance(item, dict):
        item_type = str(item.get("type") or "").lower()
        if item_type == "mcp_tool_call" and str(item.get("server") or "").lower() == "contextty":
            return True
    combined_type = f"{event_type} {item_type}"
    if not any(marker in combined_type for marker in ("tool", "mcp", "call")):
        return False
    for path, text in iter_string_fields(event):
        key = path[-1].lower() if path else ""
        lowered = text.lower()
        if "mcp__contextty" in lowered or "contextty.query_context" in lowered:
            return True
        if key in {"tool", "tool_name", "name", "recipient", "recipient_name", "server"}:
            if lowered in CONTEXTTY_TOOL_NAMES or lowered.startswith("contextty"):
                return True
    return False


def event_has_db_command(event: dict[str, Any], db_markers: set[str]) -> bool:
    for command in command_strings(event):
        normalized = command.replace("\\", "/")
        for marker in db_markers:
            if marker and marker.replace("\\", "/") in normalized:
                return True
    return False


def command_strings(event: dict[str, Any]) -> list[str]:
    event_type = str(event.get("type") or event.get("event") or "").lower()
    strings: list[str] = []
    command_keys = {"cmd", "command", "cmdline", "shell_command", "argv", "args", "arguments", "script"}
    output_keys = {"output", "stdout", "stderr", "result", "results", "message", "text", "content", "final_answer"}

    def walk(value: Any, key: str | None = None, force: bool = False) -> None:
        lowered_key = (key or "").lower()
        if lowered_key in output_keys and not force:
            return
        child_force = force or lowered_key in command_keys
        if isinstance(value, str):
            if child_force:
                strings.append(value)
        elif isinstance(value, dict):
            for child_key, child in value.items():
                walk(child, str(child_key), child_force)
        elif isinstance(value, list):
            for child in value:
                walk(child, None, child_force)

    walk(event, None, any(marker in event_type for marker in ("exec", "command", "shell")))
    return strings


def iter_string_fields(value: Any, path: tuple[str, ...] = ()) -> list[tuple[tuple[str, ...], str]]:
    fields: list[tuple[tuple[str, ...], str]] = []
    if isinstance(value, str):
        fields.append((path, value))
    elif isinstance(value, dict):
        for key, child in value.items():
            fields.extend(iter_string_fields(child, (*path, str(key))))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            fields.extend(iter_string_fields(child, (*path, str(index))))
    return fields


def parse_final_json(text: str) -> dict[str, Any]:
    stripped = text.strip()
    if not stripped:
        raise ValueError("empty final answer")
    try:
        parsed = json.loads(stripped)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", stripped, flags=re.IGNORECASE | re.DOTALL)
    if fenced:
        parsed = json.loads(fenced.group(1))
        if isinstance(parsed, dict):
            return parsed

    decoder = json.JSONDecoder()
    for match in re.finditer(r"\{", stripped):
        try:
            parsed, _end = decoder.raw_decode(stripped[match.start() :])
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed
    raise ValueError("final answer did not contain a JSON object")


def score_payload(payload: dict[str, Any], ground_truth: dict[str, dict[str, Any]]) -> dict[str, ScoreResult]:
    raw_answers = payload.get("answers")
    if not isinstance(raw_answers, list):
        raw_answers = []
    answers_by_id: dict[str, dict[str, Any]] = {}
    for raw_answer in raw_answers:
        if isinstance(raw_answer, dict) and raw_answer.get("id") is not None:
            answers_by_id[str(raw_answer["id"]).upper()] = raw_answer

    scores: dict[str, ScoreResult] = {}
    for question_id, truth in ground_truth.items():
        scores[question_id] = score_answer(question_id, truth, answers_by_id.get(question_id))
    return scores


def score_answer(question_id: str, truth: dict[str, Any], answer: dict[str, Any] | None) -> ScoreResult:
    expected = truth["expected"]
    if answer is None:
        return ScoreResult(
            id=question_id,
            correct=False,
            insufficient_context=False,
            source_used="missing",
            confidence=None,
            reason="missing answer",
            expected=expected,
            actual=None,
        )

    actual = answer.get("answer")
    source_used = normalize_source(answer.get("source_used"))
    confidence = float_or_none(answer.get("confidence"))
    answer_text = value_text(actual)
    if is_insufficient_answer(answer_text, source_used):
        return ScoreResult(
            id=question_id,
            correct=False,
            insufficient_context=True,
            source_used=source_used,
            confidence=confidence,
            reason="insufficient context",
            expected=expected,
            actual=actual,
        )

    expected_kind = truth["expected_kind"]
    correct, reason = validate_expected(expected, expected_kind, answer_text)
    return ScoreResult(
        id=question_id,
        correct=correct,
        insufficient_context=False,
        source_used=source_used,
        confidence=confidence,
        reason=reason,
        expected=expected,
        actual=actual,
    )


def validate_expected(expected: Any, expected_kind: str, answer_text: str) -> tuple[bool, str]:
    if expected_kind == "number":
        return validate_number(expected, answer_text)
    if expected_kind == "value":
        return validate_value(expected, answer_text)
    if expected_kind == "list":
        missing = [item for item in expected if not contains_value(answer_text, item)]
        return (not missing, f"missing values: {missing}" if missing else "matched all values")
    if expected_kind == "mapping":
        missing: list[str] = []
        for key, value in expected.items():
            if not contains_value(answer_text, key) or not contains_value(answer_text, value):
                missing.append(f"{key}={value}")
        return (not missing, f"missing mappings: {missing}" if missing else "matched mapping")
    if expected_kind == "rows":
        missing_rows: list[dict[str, Any]] = []
        for row in expected:
            if not all(contains_value(answer_text, value) for value in row.values()):
                missing_rows.append(row)
        return (not missing_rows, f"missing rows: {missing_rows}" if missing_rows else "matched rows")
    return False, f"unsupported expected kind: {expected_kind}"


def validate_number(expected: Any, answer_text: str) -> tuple[bool, str]:
    if expected is None:
        return validate_value(expected, answer_text)
    if not isinstance(expected, (int, float)):
        return validate_value(expected, answer_text)
    if contains_number(answer_text, float(expected), integral=isinstance(expected, int)):
        return True, "matched number"
    return False, f"expected number {expected}"


def validate_value(expected: Any, answer_text: str) -> tuple[bool, str]:
    if contains_value(answer_text, expected):
        return True, "matched value"
    return False, f"expected value {expected}"


def contains_value(answer_text: str, expected: Any) -> bool:
    if expected is None:
        return any(marker in normalize_text(answer_text).split() for marker in ("none", "null"))
    if isinstance(expected, (int, float)) and not isinstance(expected, bool):
        return contains_number(answer_text, float(expected), integral=isinstance(expected, int))
    expected_text = str(expected)
    return normalize_text(expected_text) in normalize_text(answer_text)


def contains_number(answer_text: str, expected: float, integral: bool = False) -> bool:
    for match in re.finditer(r"-?\d+(?:,\d{3})*(?:\.\d+)?|-?\d+(?:\.\d+)?", answer_text):
        raw = match.group(0).replace(",", "")
        try:
            number = float(raw)
        except ValueError:
            continue
        tolerance = 0.0 if integral else max(0.01, abs(expected) * 1e-6)
        if abs(number - expected) <= tolerance:
            return True
    return False


def normalize_text(value: str) -> str:
    return " ".join(re.sub(r"[^a-z0-9]+", " ", value.lower()).split())


def value_text(value: Any) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return "" if value is None else str(value)


def normalize_source(value: Any) -> str:
    text = normalize_text(str(value or "unknown"))
    if "insufficient" in text:
        return "insufficient_context"
    if "direct" in text or "live db" in text:
        return "direct_db"
    if "fallback" in text or "sqlite" in text or text == "db":
        return "db_fallback"
    if "both" in text:
        return "both"
    if "contextty" in text or "mcp" in text or "snapshot" in text:
        return "contextty"
    return text.replace(" ", "_") or "unknown"


def float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def is_insufficient_answer(answer_text: str, source_used: str) -> bool:
    normalized = normalize_text(answer_text)
    return "insufficient context" in normalized or answer_text.strip().upper() == "INSUFFICIENT_CONTEXT" or source_used == "insufficient_context"


def read_default_codex_model(config_path: Path | str | None = None) -> str | None:
    path = Path(config_path) if config_path is not None else Path.home() / ".codex" / "config.toml"
    if not path.exists():
        return None
    try:
        import tomllib

        data = tomllib.loads(path.read_text(encoding="utf-8"))
        model = data.get("model")
        return str(model) if model else None
    except Exception:
        match = re.search(r'(?m)^\s*model\s*=\s*"([^"]+)"', path.read_text(encoding="utf-8", errors="replace"))
        return match.group(1) if match else None


def codex_mcp_config_overrides(store_path: Path | str) -> list[str]:
    return [
        'mcp_servers.contextty.command="python3"',
        'mcp_servers.contextty.args=["-m","contextty.cli","serve","--mcp"]',
        'mcp_servers.contextty.default_tools_approval_mode="approve"',
        f"mcp_servers.contextty.env.PYTHONPATH={json.dumps(str(SRC_ROOT))}",
        f"mcp_servers.contextty.env.CONTEXTTY_STORE_PATH={json.dumps(str(Path(store_path).resolve()))}",
    ]


def run_codex_lane(
    lane: LaneName,
    codex_bin: str,
    model: str | None,
    db_path: Path,
    source_name: str,
    store_path: Path,
    output_dir: Path,
    ground_truth: dict[str, dict[str, Any]],
    schema_path: Path,
    codex_timeout_seconds: float,
) -> LaneResult:
    lane_dir = output_dir / lane
    lane_dir.mkdir(parents=True, exist_ok=True)
    prompt = build_prompt(lane, db_path, source_name)
    prompt_path = lane_dir / "prompt.md"
    raw_jsonl_path = lane_dir / "codex.jsonl"
    final_answer_path = lane_dir / "final_answer.txt"
    parsed_answer_path = lane_dir / "final_answer.json"
    stderr_path = lane_dir / "stderr.txt"
    command_path = lane_dir / "command.json"
    prompt_path.write_text(prompt, encoding="utf-8")

    cmd = [
        codex_bin,
        "exec",
        "--json",
        "--ephemeral",
        "--ignore-user-config",
        "--ignore-rules",
        "--cd",
        str(REPO_ROOT),
        "--sandbox",
        "workspace-write",
        "--output-schema",
        str(schema_path),
        "--output-last-message",
        str(final_answer_path),
    ]
    if model:
        cmd.extend(["--model", model])
    if lane != "direct_db":
        for override in codex_mcp_config_overrides(store_path):
            cmd.extend(["-c", override])
    cmd.append("-")
    command_path.write_text(json.dumps(cmd, indent=2), encoding="utf-8")

    started_at = time.monotonic()
    try:
        with raw_jsonl_path.open("w", encoding="utf-8") as stdout, stderr_path.open("w", encoding="utf-8") as stderr:
            process = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=stdout,
                stderr=stderr,
                text=True,
                cwd=REPO_ROOT,
                env=os.environ.copy(),
                start_new_session=True,
            )
            try:
                process.communicate(prompt, timeout=codex_timeout_seconds)
                returncode: int | None = process.returncode
                run_error = None
            except subprocess.TimeoutExpired:
                terminate_process_group(process)
                returncode = None
                run_error = f"codex timed out after {codex_timeout_seconds:g}s"
    except FileNotFoundError as exc:
        returncode = None
        run_error = str(exc)
        stderr_path.write_text(str(exc), encoding="utf-8")
    wall_clock_ms = int((time.monotonic() - started_at) * 1000)

    events = parse_codex_jsonl(raw_jsonl_path)
    metrics = extract_metrics(events, wall_clock_ms=wall_clock_ms)
    trace = analyze_tool_trace(events, db_path, lane)
    scores: dict[str, ScoreResult] = {}
    try:
        final_text = final_answer_path.read_text(encoding="utf-8")
        parsed_answer = parse_final_json(final_text)
        parsed_answer_path.write_text(json.dumps(parsed_answer, indent=2, sort_keys=True), encoding="utf-8")
        scores = score_payload(parsed_answer, ground_truth)
    except Exception as exc:
        run_error = run_error or str(exc)
        parsed_answer_path.write_text(json.dumps({"error": str(exc)}, indent=2), encoding="utf-8")
        scores = {
            question_id: ScoreResult(
                id=question_id,
                correct=False,
                insufficient_context=False,
                source_used="missing",
                confidence=None,
                reason=f"unscored: {exc}",
                expected=truth["expected"],
                actual=None,
            )
            for question_id, truth in ground_truth.items()
        }

    return LaneResult(
        lane=lane,
        returncode=returncode,
        prompt_path=str(prompt_path),
        raw_jsonl_path=str(raw_jsonl_path),
        final_answer_path=str(final_answer_path),
        stderr_path=str(stderr_path),
        parsed_answer_path=str(parsed_answer_path),
        metrics=metrics,
        trace=trace,
        scores=scores,
        error=run_error,
    )


def terminate_process_group(process: subprocess.Popen[str]) -> None:
    try:
        os.killpg(process.pid, 15)
        process.wait(timeout=5)
    except Exception:
        try:
            os.killpg(process.pid, 9)
        except Exception:
            process.kill()
        process.wait(timeout=5)


def lane_summary(result: LaneResult) -> dict[str, Any]:
    total = len(result.scores)
    correct = sum(1 for score in result.scores.values() if score.correct)
    insufficient = sum(1 for score in result.scores.values() if score.insufficient_context)
    fallback_count = sum(1 for score in result.scores.values() if score.source_used in {"db_fallback", "both"})
    mcp_answer_count = sum(
        1 for score in result.scores.values() if score.correct and score.source_used in {"contextty", "both"}
    )
    return {
        "lane": result.lane,
        "correct": correct,
        "questions": total,
        "accuracy": correct / total if total else 0,
        "insufficient_context": insufficient,
        "fallback_count": fallback_count,
        "mcp_answer_count": mcp_answer_count,
        "returncode": result.returncode,
        "error": result.error,
        "metrics": asdict(result.metrics),
        "trace": asdict(result.trace),
    }


def build_report_json(
    snapshot: SnapshotStats,
    ground_truth: dict[str, dict[str, Any]],
    results: dict[str, LaneResult],
) -> dict[str, Any]:
    return {
        "snapshot": asdict(snapshot),
        "ground_truth": ground_truth,
        "lanes": {
            lane: {
                **lane_summary(result),
                "scores": {question_id: asdict(score) for question_id, score in result.scores.items()},
                "paths": {
                    "prompt": result.prompt_path,
                    "raw_jsonl": result.raw_jsonl_path,
                    "final_answer": result.final_answer_path,
                    "parsed_answer": result.parsed_answer_path,
                    "stderr": result.stderr_path,
                },
            }
            for lane, result in results.items()
        },
    }


def build_report_markdown(
    snapshot: SnapshotStats,
    ground_truth: dict[str, dict[str, Any]],
    results: dict[str, LaneResult],
) -> str:
    lines = ["# Contextty Accuracy Benchmark", ""]
    lines.extend(
        [
            "## Snapshot",
            "",
            "| store | source | profile | row limit | nodes | edges | pills | facts |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
            (
                f"| `{snapshot.store_path}` | `{snapshot.source_name}` | {snapshot.profile_mode} | "
                f"{snapshot.row_limit} | {snapshot.nodes} | {snapshot.edges} | {snapshot.pills} | {snapshot.facts} |"
            ),
            "",
        ]
    )

    lines.extend(
        [
            "## Lane Summary",
            "",
            (
                "| lane | status | correct | tokens | input | output | reasoning | wall s | task s | ttfb s | "
                "MCP used | DB accessed | MCP answers | fallbacks | violations |"
            ),
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: | --- |",
        ]
    )
    for lane in VALID_LANES:
        result = results.get(lane)
        if not result:
            continue
        summary = lane_summary(result)
        metrics = result.metrics
        trace = result.trace
        violations = []
        if trace.contextty_only_db_violation:
            violations.append("contextty_only_db")
        if lane == "contextty_then_db" and not trace.hybrid_order_valid:
            violations.append("hybrid_order")
        lines.append(
            "| {lane} | {status} | {correct}/{total} | {tokens} | {input_tokens} | {output_tokens} | {reasoning_tokens} | "
            "{wall} | {duration} | {ttfb} | {mcp} | {db} | {mcp_answers} | {fallbacks} | {violations} |".format(
                lane=lane,
                status=lane_status(result),
                correct=summary["correct"],
                total=summary["questions"],
                tokens=fmt_int(metrics.total_tokens),
                input_tokens=fmt_int(metrics.input_tokens),
                output_tokens=fmt_int(metrics.output_tokens),
                reasoning_tokens=fmt_int(metrics.reasoning_tokens),
                wall=fmt_seconds(metrics.wall_clock_ms),
                duration=fmt_seconds(metrics.duration_ms),
                ttfb=fmt_seconds(metrics.time_to_first_token_ms),
                mcp=yes_no(trace.mcp_used),
                db=yes_no(trace.db_accessed),
                mcp_answers=summary["mcp_answer_count"],
                fallbacks=summary["fallback_count"],
                violations=", ".join(violations) if violations else "",
            )
        )
    lines.append("")
    failures = [result for result in results.values() if result.returncode not in (0, None) or result.error]
    if failures:
        lines.extend(["## Run Failures", ""])
        for result in failures:
            error = result.error or f"codex exited with status {result.returncode}"
            lines.append(f"- `{result.lane}`: {error}")
        lines.append("")

    if "direct_db" in results:
        baseline = lane_summary(results["direct_db"])
        lines.extend(
            [
                "## Deltas Vs Direct DB",
                "",
                "| lane | correct delta | token delta | wall delta s |",
                "| --- | ---: | ---: | ---: |",
            ]
        )
        for lane in ("contextty_only", "contextty_then_db"):
            if lane not in results:
                continue
            summary = lane_summary(results[lane])
            token_delta = none_diff(results[lane].metrics.total_tokens, results["direct_db"].metrics.total_tokens)
            wall_delta = none_diff(results[lane].metrics.wall_clock_ms, results["direct_db"].metrics.wall_clock_ms)
            lines.append(
                f"| {lane} | {summary['correct'] - baseline['correct']} | {fmt_signed_int(token_delta)} | {fmt_signed_seconds(wall_delta)} |"
            )
        lines.append("")

    lines.extend(
        [
            "## Per Question",
            "",
            "| id | category | direct DB | Contextty only | hybrid | hybrid source | Contextty right |",
            "| --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for question_id in ground_truth:
        truth = ground_truth[question_id]
        direct_score = maybe_score(results, "direct_db", question_id)
        contextty_score = maybe_score(results, "contextty_only", question_id)
        hybrid_score = maybe_score(results, "contextty_then_db", question_id)
        contextty_right = contextty_statement(contextty_score, hybrid_score)
        lines.append(
            "| {id} | {category} | {direct} | {contextty} | {hybrid} | {hybrid_source} | {contextty_right} |".format(
                id=question_id,
                category=truth["category"].replace("_", "/"),
                direct=score_mark(direct_score),
                contextty=score_mark(contextty_score),
                hybrid=score_mark(hybrid_score),
                hybrid_source=hybrid_score.source_used if hybrid_score else "",
                contextty_right=contextty_right,
            )
        )
    lines.append("")
    lines.append("Contextty MCP is counted as providing the right answer when Contextty-only is correct, or when the hybrid answer is correct and reports `contextty` or `both` as its source.")
    lines.append("")
    return "\n".join(lines)


def maybe_score(results: dict[str, LaneResult], lane: str, question_id: str) -> ScoreResult | None:
    result = results.get(lane)
    if not result:
        return None
    return result.scores.get(question_id)


def score_mark(score: ScoreResult | None) -> str:
    if score is None:
        return ""
    if score.correct:
        return "yes"
    if score.insufficient_context:
        return "insufficient"
    return "no"


def lane_status(result: LaneResult) -> str:
    if result.returncode == 0 and not result.error:
        return "ok"
    if result.returncode is None:
        return "failed"
    return f"failed ({result.returncode})"


def contextty_statement(contextty_score: ScoreResult | None, hybrid_score: ScoreResult | None) -> str:
    if contextty_score and contextty_score.correct:
        return "yes"
    if hybrid_score and hybrid_score.correct and hybrid_score.source_used in {"contextty", "both"}:
        return "yes"
    if contextty_score and contextty_score.insufficient_context:
        return "insufficient"
    return "no"


def fmt_int(value: int | None) -> str:
    return "" if value is None else str(value)


def fmt_seconds(ms: int | None) -> str:
    if ms is None:
        return ""
    return f"{ms / 1000:.2f}"


def fmt_signed_seconds(ms: int | None) -> str:
    if ms is None:
        return ""
    return f"{ms / 1000:+.2f}"


def fmt_signed_int(value: int | None) -> str:
    if value is None:
        return ""
    return f"{value:+d}"


def none_diff(value: int | None, baseline: int | None) -> int | None:
    if value is None or baseline is None:
        return None
    return value - baseline


def yes_no(value: bool) -> str:
    return "yes" if value else "no"


def benchmark_run_succeeded(results: dict[str, LaneResult]) -> bool:
    return all(result.returncode == 0 and not result.error for result in results.values())


def build_strategy_results(snapshot: SnapshotStats, results: dict[str, LaneResult]) -> dict[str, Any]:
    direct = results.get("direct_db")
    hybrid = results.get("contextty_then_db")
    contextty_only = results.get("contextty_only")
    return {
        "active_strategy": ACTIVE_STRATEGY,
        "strategy_order": list(STRATEGY_ORDER),
        "snapshot": {
            "nodes": snapshot.nodes,
            "edges": snapshot.edges,
            "pills": snapshot.pills,
            "facts": snapshot.facts,
            "row_limit": snapshot.row_limit,
        },
        "lanes": {lane: lane_summary(result) for lane, result in results.items()},
        "hybrid_vs_direct": {
            "correct_delta": (
                lane_summary(hybrid)["correct"] - lane_summary(direct)["correct"]
                if direct and hybrid
                else None
            ),
            "token_delta": none_diff(hybrid.metrics.total_tokens, direct.metrics.total_tokens) if direct and hybrid else None,
            "wall_ms_delta": none_diff(hybrid.metrics.wall_clock_ms, direct.metrics.wall_clock_ms) if direct and hybrid else None,
        },
        "contextty_only_correct": lane_summary(contextty_only)["correct"] if contextty_only else None,
    }


def parse_lanes(raw: str) -> list[LaneName]:
    lanes = [lane.strip() for lane in raw.split(",") if lane.strip()]
    invalid = [lane for lane in lanes if lane not in VALID_LANES]
    if invalid:
        raise argparse.ArgumentTypeError(f"unsupported lanes: {', '.join(invalid)}")
    if not lanes:
        raise argparse.ArgumentTypeError("at least one lane is required")
    return lanes  # type: ignore[return-value]


def default_output_dir() -> Path:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return BENCHMARK_ROOT / timestamp


def cleanup_generated_benchmark_artifacts(
    latest_output_dir: Path,
    keep_latest: int = 1,
    manifest_path: Path = GENERATED_RUNS_MANIFEST,
) -> list[Path]:
    if keep_latest < 1:
        return []

    benchmark_root = manifest_path.parent.resolve()
    latest_output_dir = latest_output_dir.resolve()
    if not latest_output_dir.is_relative_to(benchmark_root):
        return []

    generated = read_generated_runs_manifest(manifest_path)
    generated.append(latest_output_dir)
    existing = sorted(
        {path.resolve() for path in generated if path.exists() and path.resolve().is_relative_to(benchmark_root)},
        key=lambda path: (path.stat().st_mtime, path.name),
        reverse=True,
    )
    keep = set(existing[:keep_latest])
    removed: list[Path] = []
    for path in existing[keep_latest:]:
        if path == latest_output_dir or path in keep:
            continue
        shutil.rmtree(path)
        removed.append(path)

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps([str(path) for path in existing if path in keep], indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return removed


def read_generated_runs_manifest(manifest_path: Path = GENERATED_RUNS_MANIFEST) -> list[Path]:
    if not manifest_path.exists():
        return []
    try:
        raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    if not isinstance(raw, list):
        return []
    return [Path(item) for item in raw if isinstance(item, str)]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a local Contextty accuracy benchmark with Codex lanes.")
    parser.add_argument("--db-path", type=Path, default=REPO_ROOT / ".test" / "contextty_test.db")
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--source-name", default="contextty-test")
    parser.add_argument("--row-limit", type=int, default=1000)
    parser.add_argument("--codex-bin", default="codex")
    parser.add_argument("--codex-timeout", type=float, default=600.0)
    parser.add_argument("--model", default=None)
    parser.add_argument("--lanes", type=parse_lanes, default=list(VALID_LANES))
    parser.add_argument(
        "--keep-latest-artifacts",
        type=int,
        default=1,
        help="Keep this many benchmark runs produced by this script and remove older generated runs.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    output_dir = (args.output_dir or default_output_dir()).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    db_path = args.db_path.expanduser().resolve()
    if not db_path.exists():
        print(f"database not found: {db_path}", file=sys.stderr)
        return 2

    model = args.model or read_default_codex_model()
    schema_path = output_dir / "answer_schema.json"
    schema_path.write_text(json.dumps(ANSWER_SCHEMA, indent=2, sort_keys=True), encoding="utf-8")

    ground_truth = compute_ground_truth(db_path)
    (output_dir / "ground_truth.json").write_text(json.dumps(ground_truth, indent=2, sort_keys=True), encoding="utf-8")

    snapshot = setup_contextty_snapshot(
        db_path=db_path,
        output_dir=output_dir,
        source_name=args.source_name,
        row_limit=args.row_limit,
    )
    store_path = Path(snapshot.store_path)

    results: dict[str, LaneResult] = {}
    for lane in args.lanes:
        print(f"running lane {lane}...", file=sys.stderr)
        results[lane] = run_codex_lane(
            lane=lane,
            codex_bin=args.codex_bin,
            model=model,
            db_path=db_path,
            source_name=args.source_name,
            store_path=store_path,
            output_dir=output_dir,
            ground_truth=ground_truth,
            schema_path=schema_path,
            codex_timeout_seconds=args.codex_timeout,
        )

    report_json = build_report_json(snapshot, ground_truth, results)
    (output_dir / "report.json").write_text(json.dumps(report_json, indent=2, sort_keys=True), encoding="utf-8")
    (output_dir / "report.md").write_text(build_report_markdown(snapshot, ground_truth, results), encoding="utf-8")
    (output_dir / "strategy_results.json").write_text(
        json.dumps(build_strategy_results(snapshot, results), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    succeeded = benchmark_run_succeeded(results)
    if succeeded and set(args.lanes) == set(VALID_LANES):
        removed = cleanup_generated_benchmark_artifacts(output_dir, keep_latest=args.keep_latest_artifacts)
        for path in removed:
            print(f"removed old benchmark artifact: {path}", file=sys.stderr)
    elif succeeded:
        print("partial lane run succeeded; skipping artifact cleanup", file=sys.stderr)
    else:
        print("benchmark did not complete successfully; skipping artifact cleanup", file=sys.stderr)
    print(f"report: {output_dir / 'report.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
