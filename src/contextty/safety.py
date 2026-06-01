from __future__ import annotations

import re

READONLY_STARTERS = {"select", "with", "show", "explain"}
FORBIDDEN_TOKENS = {
    "alter",
    "analyze",
    "call",
    "cluster",
    "comment",
    "copy",
    "create",
    "delete",
    "do",
    "drop",
    "execute",
    "grant",
    "insert",
    "listen",
    "lock",
    "merge",
    "notify",
    "reindex",
    "refresh",
    "reset",
    "revoke",
    "security",
    "set",
    "truncate",
    "update",
    "vacuum",
}
FORBIDDEN_FUNCTIONS = {"nextval", "setval"}


class UnsafeSQLError(ValueError):
    pass


def strip_sql_comments_and_literals(sql: str) -> str:
    output: list[str] = []
    i = 0
    state = "normal"
    dollar_tag: str | None = None
    while i < len(sql):
        char = sql[i]
        nxt = sql[i + 1] if i + 1 < len(sql) else ""

        if state == "normal":
            if char == "-" and nxt == "-":
                state = "line_comment"
                output.append(" ")
                i += 2
                continue
            if char == "/" and nxt == "*":
                state = "block_comment"
                output.append(" ")
                i += 2
                continue
            if char == "'":
                state = "single_quote"
                output.append(" ")
                i += 1
                continue
            if char == '"':
                state = "double_quote"
                output.append(" ")
                i += 1
                continue
            if char == "$":
                match = re.match(r"\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$", sql[i:])
                if match:
                    dollar_tag = match.group(0)
                    state = "dollar_quote"
                    output.append(" ")
                    i += len(dollar_tag)
                    continue
            output.append(char)
            i += 1
            continue

        if state == "line_comment":
            if char == "\n":
                state = "normal"
                output.append("\n")
            i += 1
            continue

        if state == "block_comment":
            if char == "*" and nxt == "/":
                state = "normal"
                i += 2
            else:
                i += 1
            continue

        if state == "single_quote":
            if char == "'" and nxt == "'":
                i += 2
                continue
            if char == "'":
                state = "normal"
            i += 1
            continue

        if state == "double_quote":
            if char == '"' and nxt == '"':
                i += 2
                continue
            if char == '"':
                state = "normal"
            i += 1
            continue

        if state == "dollar_quote":
            if dollar_tag and sql.startswith(dollar_tag, i):
                i += len(dollar_tag)
                state = "normal"
            else:
                i += 1
            continue

    return "".join(output)


def validate_readonly_sql(sql: str) -> str:
    cleaned = strip_sql_comments_and_literals(sql).strip()
    if not cleaned:
        raise UnsafeSQLError("SQL is empty")

    statements = [part.strip() for part in cleaned.split(";") if part.strip()]
    if len(statements) != 1:
        raise UnsafeSQLError("only one read-only SQL statement is allowed")

    tokens = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", statements[0].lower())
    if not tokens:
        raise UnsafeSQLError("SQL has no readable command token")
    if tokens[0] not in READONLY_STARTERS:
        raise UnsafeSQLError(f"SQL must start with one of: {', '.join(sorted(READONLY_STARTERS))}")

    forbidden = FORBIDDEN_TOKENS.intersection(tokens)
    if forbidden:
        raise UnsafeSQLError(f"SQL contains mutating or session-changing token: {sorted(forbidden)[0]}")

    lowered = statements[0].lower()
    for function in FORBIDDEN_FUNCTIONS:
        if re.search(rf"\b{re.escape(function)}\s*\(", lowered):
            raise UnsafeSQLError(f"SQL contains unsafe function: {function}")

    if re.search(r"\bselect\s+.*\binto\b", lowered, flags=re.DOTALL):
        raise UnsafeSQLError("SELECT INTO is not allowed")

    return sql

