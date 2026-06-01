from __future__ import annotations

import re


def parse_timeout(value: str | float | int) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    text = value.strip().lower()
    if text.endswith("ms"):
        return float(text[:-2]) / 1000
    if text.endswith("s"):
        return float(text[:-1])
    if text.endswith("m"):
        return float(text[:-1]) * 60
    return float(text)


def text_patterns(values: list[str], limit: int = 10) -> list[dict[str, int | str]]:
    buckets: dict[str, int] = {}
    for value in values:
        tokens = _pattern_tokens(value)
        template = " ".join(tokens)
        buckets[template] = buckets.get(template, 0) + 1
    return [
        {"template": template, "count": count}
        for template, count in sorted(buckets.items(), key=lambda item: (-item[1], item[0]))[:limit]
    ]


def _pattern_tokens(value: str) -> list[str]:
    words = re.findall(r"[A-Za-z]+|\d+|[0-9a-fA-F-]{8,}|[^\w\s]", value[:500])
    tokens: list[str] = []
    for word in words:
        lowered = word.lower()
        if re.fullmatch(r"\d+", lowered):
            tokens.append("<num>")
        elif re.fullmatch(r"[0-9a-f]{8,}(?:-[0-9a-f]{4,})*", lowered):
            tokens.append("<id>")
        elif len(lowered) > 32:
            tokens.append("<text>")
        else:
            tokens.append(lowered)
    return tokens or ["<empty>"]
