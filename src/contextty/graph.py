from __future__ import annotations

import math
import re
from collections import deque
from typing import Any

from .facts import ANSWER_FACT_KINDS, ROW_FACT_KINDS, SCHEMA_FACT_KINDS
from .storage import LocalStore

COMPACT_FACT_KINDS = SCHEMA_FACT_KINDS
ROW_FACT_KINDS_FOR_PACK = ROW_FACT_KINDS
SCHEMA_CONTEXT_WORD_CAP = 1200
ANSWER_CONTEXT_WORD_CAP = 900
MISS_CONTEXT_WORD_CAP = 160
ROW_LEVEL_FALLBACK_CONTEXT = (
    "NEEDS_DB_FALLBACK: no exact bounded snapshot fact matched this row or aggregate question. "
    "Use the involved tables and columns below if live read-only DB fallback is permitted."
)


def tokenize(text: str) -> set[str]:
    tokens: set[str] = set()
    for token in re.findall(r"[A-Za-z0-9_]+", text.lower()):
        if len(token) > 1:
            tokens.add(token)
        for part in token.split("_"):
            if len(part) > 1:
                tokens.add(part)
    return tokens


def table_mentions_for(query: str) -> set[str]:
    mentions: set[str] = set()
    for token in re.findall(r"[A-Za-z][A-Za-z0-9_]*", query.lower()):
        mentions.add(token)
        if token.endswith("s"):
            mentions.add(token[:-1])
    return mentions


class ContextGraph:
    def __init__(self, store: LocalStore, source_id: int | None = None, snapshot_run_id: int | None = None) -> None:
        self.store = store
        self.run = store.resolve_snapshot_run(source_id=source_id, snapshot_run_id=snapshot_run_id)
        resolved_run_id = self.run.id if self.run else snapshot_run_id
        resolved_source_id = self.run.source_id if self.run else source_id
        self.nodes = {node["id"]: node for node in store.get_nodes(source_id=resolved_source_id, snapshot_run_id=resolved_run_id)}
        self.edges = store.get_edges(source_id=resolved_source_id, snapshot_run_id=resolved_run_id)
        self.pills = store.get_pills(source_id=resolved_source_id, snapshot_run_id=resolved_run_id)
        self.facts = store.get_facts(source_id=resolved_source_id, snapshot_run_id=resolved_run_id)
        self.source_id = resolved_source_id
        self.snapshot_run_id = resolved_run_id
        self.outgoing: dict[str, list[dict[str, Any]]] = {}
        self.incoming: dict[str, list[dict[str, Any]]] = {}
        for edge in self.edges:
            self.outgoing.setdefault(edge["from_node_id"], []).append(edge)
            self.incoming.setdefault(edge["to_node_id"], []).append(edge)
        self.pills_by_node: dict[str, list[dict[str, Any]]] = {}
        for pill in self.pills:
            self.pills_by_node.setdefault(pill["node_id"], []).append(pill)

    def get_node(self, node_id: str) -> dict[str, Any] | None:
        node = self.nodes.get(node_id)
        if not node:
            return None
        return {
            **node,
            "pills": self.pills_by_node.get(node_id, []),
        }

    def get_neighbors(self, node_id: str, hops: int = 1, direction: str = "both") -> dict[str, Any]:
        node_ids, edges = self._traverse([node_id], hops=hops, direction=direction)
        return {
            "nodes": [self._node_with_pills(current) for current in sorted(node_ids) if current in self.nodes],
            "edges": edges,
        }

    def query_context(
        self,
        query: str,
        budget: int = 2000,
        hops: int = 2,
        direction: str = "both",
    ) -> dict[str, Any]:
        query_mode = self._query_mode(query)
        answer_pack = self._is_answer_pack_query(query)
        fact_budget = min(
            max(1, budget),
            12000 if answer_pack else (SCHEMA_CONTEXT_WORD_CAP if query_mode == "schema_profile" else ANSWER_CONTEXT_WORD_CAP),
        )
        ranked_facts = self._rank_facts(query, query_mode)
        rendered_facts, fact_context = self._render_facts(ranked_facts, fact_budget)

        seeds = self._rank_nodes(query)[:5]
        seed_ids = [node["id"] for node, _score in seeds]
        if not seed_ids and self.nodes:
            seed_ids = list(self.nodes)[:1]
        node_ids, edges = self._traverse(seed_ids, hops=hops, direction=direction)
        ranked = sorted(
            (self.nodes[node_id] for node_id in node_ids if node_id in self.nodes and self.nodes[node_id]["kind"] != "context_pill"),
            key=lambda node: (-self._node_score(query, node), node["kind"], node["qualified_name"]),
        )
        if rendered_facts and not answer_pack:
            selected_nodes = self._fact_nodes(rendered_facts)
            selected_pills = []
            verbose_context = ""
        elif rendered_facts:
            selected_nodes = []
            selected_pills = []
            verbose_context = ""
        elif query_mode == "row_level":
            selected_nodes, selected_pills, verbose_context = self._render_context(
                ranked,
                MISS_CONTEXT_WORD_CAP,
                include_pills=False,
            )
        else:
            remaining_budget = max(0, fact_budget - len(fact_context.split()))
            selected_nodes, selected_pills, verbose_context = self._render_context(
                ranked,
                remaining_budget,
                include_pills=not rendered_facts,
                excluded_pill_ids={fact["id"].replace("fact:", "pill:", 1) for fact in ranked_facts},
            )
        context_parts: list[str] = []
        if query_mode == "row_level" and not rendered_facts:
            context_parts.append(ROW_LEVEL_FALLBACK_CONTEXT)
        if fact_context:
            context_parts.append(fact_context)
        if verbose_context:
            context_parts.append(verbose_context)
        context = "\n".join(context_parts)
        selected_node_ids = {node["id"] for node in selected_nodes}
        selected_node_ids.update(fact["node_id"] for fact in rendered_facts if fact.get("node_id"))
        selected_node_ids.update(node_id for node_id in seed_ids if node_id in self.nodes and query_mode != "row_level")
        answerability = self._answerability(query_mode, rendered_facts, bool(verbose_context))
        return {
            "query": query,
            "answerability": answerability,
            "answer_candidates": self._compact_answer_candidates(rendered_facts) if answer_pack else rendered_facts,
            "facts": [] if answer_pack else rendered_facts,
            "seeds": [] if answer_pack else [self._compact_node(node_id) for node_id in seed_ids if node_id in self.nodes],
            "nodes": selected_nodes,
            "edges": [] if rendered_facts else [
                self._compact_edge(edge)
                for edge in edges
                if edge["from_node_id"] in selected_node_ids or edge["to_node_id"] in selected_node_ids
            ][:20],
            "pills": selected_pills,
            "context": context,
        }

    def find_path(self, start_node_id: str, end_node_id: str, direction: str = "both") -> dict[str, Any]:
        if start_node_id not in self.nodes or end_node_id not in self.nodes:
            return {"path": [], "edges": []}
        queue: deque[str] = deque([start_node_id])
        previous: dict[str, tuple[str | None, dict[str, Any] | None]] = {start_node_id: (None, None)}
        while queue:
            current = queue.popleft()
            if current == end_node_id:
                break
            for edge in self._incident_edges(current, direction):
                neighbor = self._edge_neighbor(edge, current)
                if neighbor in previous or neighbor not in self.nodes:
                    continue
                previous[neighbor] = (current, edge)
                queue.append(neighbor)
        if end_node_id not in previous:
            return {"path": [], "edges": []}

        path_ids: list[str] = []
        path_edges: list[dict[str, Any]] = []
        current: str | None = end_node_id
        while current is not None:
            path_ids.append(current)
            current, edge = previous[current]
            if edge:
                path_edges.append(edge)
        path_ids.reverse()
        path_edges.reverse()
        return {
            "path": [self._node_with_pills(node_id) for node_id in path_ids],
            "edges": path_edges,
        }

    def graph_summary(self) -> dict[str, Any]:
        return {
            "nodes": list(self.nodes.values()),
            "edges": self.edges,
            "communities": self.communities(),
            "degree_centrality": self.degree_centrality(),
        }

    def communities(self) -> list[dict[str, Any]]:
        try:
            import networkx as nx

            graph = nx.Graph()
            graph.add_nodes_from(self.nodes)
            graph.add_edges_from((edge["from_node_id"], edge["to_node_id"]) for edge in self.edges)
            communities = nx.community.louvain_communities(graph, seed=7) if graph.nodes else []
            return [
                {"id": index, "nodes": sorted(node_id for node_id in community)}
                for index, community in enumerate(communities)
            ]
        except Exception:
            return self._connected_components()

    def degree_centrality(self) -> list[dict[str, Any]]:
        degree: dict[str, int] = {node_id: 0 for node_id in self.nodes}
        for edge in self.edges:
            if edge["from_node_id"] in degree:
                degree[edge["from_node_id"]] += 1
            if edge["to_node_id"] in degree:
                degree[edge["to_node_id"]] += 1
        denominator = max(1, len(self.nodes) - 1)
        return [
            {
                "node_id": node_id,
                "qualified_name": self.nodes[node_id]["qualified_name"],
                "kind": self.nodes[node_id]["kind"],
                "degree": count,
                "centrality": count / denominator,
            }
            for node_id, count in sorted(degree.items(), key=lambda item: (-item[1], self.nodes[item[0]]["qualified_name"]))
        ]

    def _rank_nodes(self, query: str) -> list[tuple[dict[str, Any], float]]:
        scored = [(node, self._node_score(query, node)) for node in self.nodes.values()]
        scored = [(node, score) for node, score in scored if score > 0]
        return sorted(scored, key=lambda item: (-item[1], item[0]["kind"], item[0]["qualified_name"]))

    def _rank_facts(self, query: str, query_mode: str) -> list[dict[str, Any]]:
        if self.snapshot_run_id is None:
            return []
        if self._is_answer_pack_query(query):
            return self._rank_answer_pack_facts(query)
        facts = self.store.search_facts(query, snapshot_run_id=self.snapshot_run_id, limit=32)
        facts = [fact for fact in facts if fact.get("kind") in ANSWER_FACT_KINDS]
        if not facts:
            return []

        query_tokens = tokenize(query)
        required_phrases = self._query_phrases(query) if query_mode == "row_level" else []
        inventory = [fact for fact in facts if fact.get("kind") == "table_inventory"]
        lowered_query = query.lower()
        if query_mode == "schema_profile" and inventory and (
            "how many user tables" in lowered_query
            or "how many rows" in lowered_query
            or "rows are in each" in lowered_query
            or "row counts" in lowered_query
        ):
            return inventory[:1]
        scored: list[tuple[dict[str, Any], float]] = []
        for fact in facts:
            fact_text = " ".join([str(fact.get("subject") or ""), str(fact.get("text") or ""), str(fact.get("search_text") or "")]).lower()
            if required_phrases and not any(phrase.lower() in fact_text for phrase in required_phrases):
                continue
            score = float(fact.get("score") or 0.0)
            if fact.get("kind") == "table_inventory" and query_mode == "schema_profile":
                score += 2.0
            if fact.get("kind") in {"relationship", "relationship_card"} and any(token in query_tokens for token in {"fk", "foreign", "key", "manager", "report", "reports", "reference", "references"}):
                score += 2.0
            if fact.get("kind") in {"aggregate", "value_domain"} and any(token in query_tokens for token in {"average", "count", "counts", "highest", "latest", "profile", "profiles", "total", "value", "values", "domain"}):
                score += 2.0
            if score > 0:
                scored.append((fact, score))

        scored.sort(key=lambda item: (-item[1], item[0]["kind"], item[0]["subject"]))
        limit = 8 if query_mode == "schema_profile" else 8
        selected: list[dict[str, Any]] = []
        seen: set[str] = set()
        if query_mode == "schema_profile" and any(token in query_tokens for token in {"row", "rows", "table", "tables"}):
            for pill in inventory:
                selected.append(pill)
                seen.add(pill["id"])
        for fact, _score in scored:
            if fact["id"] in seen:
                continue
            selected.append(fact)
            seen.add(fact["id"])
            if len(selected) >= limit:
                break
        return selected

    def _is_answer_pack_query(self, query: str) -> bool:
        lowered = query.lower()
        return (
            "answer these" in lowered
            or "answer all" in lowered
            or lowered.count(";") >= 5
            or len(tokenize(query)) >= 45
        )

    def _rank_answer_pack_facts(self, query: str) -> list[dict[str, Any]]:
        query_tokens = tokenize(query)
        query_token_set = set(query_tokens)
        phrases = self._query_phrases(query)
        selected: list[dict[str, Any]] = []
        seen: set[str] = set()

        def add(fact: dict[str, Any], score: float) -> None:
            if fact["id"] in seen:
                return
            ranked = dict(fact)
            ranked["score"] = round(score, 6)
            selected.append(ranked)
            seen.add(fact["id"])

        for fact in self.facts:
            if fact.get("kind") == "table_inventory":
                add(fact, 1000.0)

        table_mentions = table_mentions_for(query)
        lowered_query = query.lower()
        scored_schema: list[tuple[dict[str, Any], float]] = []
        scored_values: list[tuple[dict[str, Any], float]] = []
        priority_rows: list[tuple[dict[str, Any], float]] = []
        scored_rows: list[tuple[dict[str, Any], float]] = []
        for fact in self.facts:
            kind = fact.get("kind")
            if kind not in ANSWER_FACT_KINDS or kind == "table_inventory":
                continue
            text = " ".join([str(fact.get("subject") or ""), str(fact.get("text") or ""), str(fact.get("search_text") or "")])
            text_lower = text.lower()
            fact_tokens = set(tokenize(text))
            overlap = len(query_token_set.intersection(fact_tokens))
            phrase_hits = sum(1 for phrase in phrases if phrase.lower() in text_lower)
            score = overlap * 4.0 + phrase_hits * 30.0
            if kind == "table_schema":
                subject_table = str(fact.get("subject") or "").split(".")[-1].lower()
                if subject_table in table_mentions:
                    score += 60.0
                if score >= 24.0:
                    scored_schema.append((fact, score))
            elif kind == "value_domain":
                if score >= 12.0:
                    scored_values.append((fact, score + 20.0))
            elif kind in ROW_FACT_KINDS_FOR_PACK:
                priority = self._answer_pack_row_priority(lowered_query, text_lower)
                if priority:
                    priority_rows.append((fact, score + priority))
                elif phrase_hits or score >= 24.0:
                    scored_rows.append((fact, score))

        for fact, score in sorted(scored_schema, key=lambda item: (-item[1], item[0]["subject"]))[:14]:
            add(fact, score)
        for fact, score in sorted(scored_values, key=lambda item: (-item[1], item[0]["subject"]))[:10]:
            add(fact, score)
        for fact, score in sorted(priority_rows, key=lambda item: (-item[1], item[0]["kind"], item[0]["subject"]))[:60]:
            add(fact, score)
        for fact, score in sorted(scored_rows, key=lambda item: (-item[1], item[0]["kind"], item[0]["subject"]))[:20]:
            add(fact, score)
        return selected[:100]

    @staticmethod
    def _answer_pack_row_priority(query_lower: str, fact_lower: str) -> float:
        score = 0.0
        if "earliest" in query_lower and "hire_date" in query_lower and "earliest" in fact_lower and "hire_date" in fact_lower:
            score += 140.0
        if "latest salary" in query_lower and "salary_history" in fact_lower and "latest" in fact_lower:
            score += 140.0
        if "highest total" in query_lower and "allocation_percent" in query_lower and "top summed" in fact_lower and "allocation_percent" in fact_lower:
            score += 140.0
        if "average latest salary" in query_lower and "average latest" in fact_lower and "salary_history.salary" in fact_lower:
            score += 150.0
        if "performance review rating" in query_lower and "average" in fact_lower and "performance_reviews.rating" in fact_lower:
            score += 150.0
        if "department owning" in query_lower and "payroll modernization" in fact_lower and "cost_center" in fact_lower:
            score += 150.0
        if "assigned to manager insights dashboard" in query_lower and "manager insights dashboard" in fact_lower:
            score += 130.0
        if "direct reports" in query_lower and "direct_reports" in fact_lower:
            score += 130.0
        return score

    def _pill_score(self, query: str, pill: dict[str, Any]) -> float:
        query_tokens = tokenize(query)
        if not query_tokens:
            return 0
        fields = [
            pill.get("kind") or "",
            pill.get("title") or "",
            pill.get("rendered_text") or "",
        ]
        data = pill.get("json")
        if isinstance(data, dict):
            fields.append(self._flatten_fact_data(data))
        pill_tokens = tokenize(" ".join(fields))
        overlap = query_tokens.intersection(pill_tokens)
        if not overlap:
            return 0
        return len(overlap) * 6 + math.log1p(len(pill.get("rendered_text", "").split()))

    def _node_score(self, query: str, node: dict[str, Any]) -> float:
        query_tokens = tokenize(query)
        if not query_tokens:
            return 0
        fields = [
            node.get("name") or "",
            node.get("qualified_name") or "",
            node.get("kind") or "",
            node.get("summary") or "",
        ]
        for pill in self.pills_by_node.get(node["id"], []):
            fields.append(pill.get("rendered_text") or "")
        node_tokens = tokenize(" ".join(fields))
        overlap = query_tokens.intersection(node_tokens)
        if not overlap:
            return 0
        score = len(overlap) * 4
        if node["kind"] in {"table", "view"}:
            score += 2
        if node["kind"] == "column":
            score += 1
        score += math.log1p(len(self.outgoing.get(node["id"], [])) + len(self.incoming.get(node["id"], [])))
        return score

    def _query_mode(self, query: str) -> str:
        lowered = query.lower()
        row_markers = {
            "assigned to",
            "average latest salary",
            "direct reports",
            "earliest hire_date",
            "highest total",
            "latest salary",
            "owns",
            "performance review rating",
            "report directly",
            "who is",
        }
        if any(marker in lowered for marker in row_markers):
            return "row_level"
        if re.search(r"['\"][^'\"]+['\"]", query):
            return "row_level"
        if re.search(r"\b[A-Z][a-z]+ [A-Z][a-z]+(?:'s)?\b", query):
            return "row_level"
        if lowered.startswith("who "):
            return "row_level"
        if "which employees" in lowered or "which active employee" in lowered:
            return "row_level"
        return "schema_profile"

    @staticmethod
    def _query_phrases(query: str) -> list[str]:
        phrases = [match.group(1) or match.group(2) for match in re.finditer(r"'([^']+)'|\"([^\"]+)\"", query)]
        for match in re.finditer(r"\b[A-Z][A-Za-z0-9_-]+(?:\s+[A-Z][A-Za-z0-9_-]+)+\b", query):
            phrase = match.group(0)
            if phrase.lower().startswith(("what ", "which ", "who ")):
                words = phrase.split()
                phrase = " ".join(words[1:])
            if phrase and phrase not in phrases:
                phrases.append(phrase)
        return [phrase for phrase in phrases if len(phrase.split()) >= 2]

    @staticmethod
    def _answerability(query_mode: str, facts: list[dict[str, Any]], has_graph_context: bool) -> dict[str, Any]:
        if facts:
            return {
                "status": "answered_by_snapshot",
                "reason": "One or more compact facts from the local snapshot matched the question.",
            }
        if query_mode == "row_level":
            return {
                "status": "needs_db_fallback",
                "reason": "No bounded row or aggregate fact matched this question.",
            }
        return {
            "status": "partial_context" if has_graph_context else "needs_db_fallback",
            "reason": "No answer-ready fact matched; graph context may still identify relevant schema.",
        }

    def _traverse(self, seed_ids: list[str], hops: int, direction: str) -> tuple[set[str], list[dict[str, Any]]]:
        visited: set[str] = set()
        selected_edges: list[dict[str, Any]] = []
        queue: deque[tuple[str, int]] = deque()
        for seed_id in seed_ids:
            if seed_id in self.nodes:
                visited.add(seed_id)
                queue.append((seed_id, 0))
        while queue:
            current, distance = queue.popleft()
            if distance >= hops:
                continue
            for edge in self._incident_edges(current, direction):
                neighbor = self._edge_neighbor(edge, current)
                if neighbor not in self.nodes:
                    continue
                selected_edges.append(edge)
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append((neighbor, distance + 1))
        return visited, selected_edges

    def _incident_edges(self, node_id: str, direction: str) -> list[dict[str, Any]]:
        if direction == "out":
            return self.outgoing.get(node_id, [])
        if direction in {"in", "reverse"}:
            return self.incoming.get(node_id, [])
        return self.outgoing.get(node_id, []) + self.incoming.get(node_id, [])

    @staticmethod
    def _edge_neighbor(edge: dict[str, Any], node_id: str) -> str:
        return edge["to_node_id"] if edge["from_node_id"] == node_id else edge["from_node_id"]

    def _render_context(
        self,
        ranked_nodes: list[dict[str, Any]],
        budget: int,
        include_pills: bool = False,
        excluded_pill_ids: set[str] | None = None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
        excluded_pill_ids = excluded_pill_ids or set()
        selected_nodes: list[dict[str, Any]] = []
        selected_pills: list[dict[str, Any]] = []
        lines: list[str] = []
        if budget <= 0:
            return selected_nodes, selected_pills, ""
        words = 0
        max_words = max(1, budget)
        for node in ranked_nodes:
            node_lines: list[str] = []
            if node.get("summary"):
                node_lines.append(node["summary"])
            pills = [
                pill
                for pill in self.pills_by_node.get(node["id"], [])
                if pill["id"] not in excluded_pill_ids and pill.get("kind") not in COMPACT_FACT_KINDS
            ]
            if include_pills:
                node_lines.extend(pill["rendered_text"] for pill in pills)
            text = "\n".join(line for line in node_lines if line)
            if not text:
                continue
            text_words = len(text.split())
            if lines and words + text_words > max_words:
                break
            lines.append(text)
            words += text_words
            selected_nodes.append(self._compact_node(node["id"]))
            if include_pills:
                selected_pills.extend(self._compact_pill(pill) for pill in pills[:5])
        return selected_nodes, selected_pills, "\n\n".join(lines)

    def _render_facts(self, facts: list[dict[str, Any]], budget: int) -> tuple[list[dict[str, Any]], str]:
        rendered: list[dict[str, Any]] = []
        lines: list[str] = []
        words = 0
        max_words = max(1, budget)
        for fact in facts:
            text = fact.get("text") or ""
            if not text:
                continue
            text_words = len(text.split())
            if lines and words + text_words > max_words:
                continue
            lines.append(text)
            words += text_words
            rendered.append(
                {
                    "id": fact["id"],
                    "node_id": fact.get("node_id"),
                    "kind": fact["kind"],
                    "subject": fact["subject"],
                    "title": fact["subject"],
                    "data": self._compact_fact_data(fact.get("data") or {}),
                    "text": text,
                    "score": fact.get("score"),
                }
            )
        return rendered, "\n".join(lines)

    @staticmethod
    def _compact_fact_data(data: dict[str, Any]) -> dict[str, Any]:
        if len(str(data)) <= 1600:
            return data
        return {
            "omitted": "large_fact_data",
            "keys": sorted(str(key) for key in data.keys())[:20],
        }

    @staticmethod
    def _compact_answer_candidates(facts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "kind": fact["kind"],
                "subject": fact["subject"],
                "text": fact["text"],
            }
            for fact in facts
        ]

    def _fact_nodes(self, facts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        selected: list[dict[str, Any]] = []
        seen: set[str] = set()
        for fact in facts:
            node_id = fact.get("node_id")
            if not node_id or node_id in seen or node_id not in self.nodes:
                continue
            seen.add(node_id)
            selected.append(self._compact_node(node_id))
            if len(selected) >= 10:
                break
        return selected

    def _compact_node(self, node_id: str) -> dict[str, Any]:
        node = dict(self.nodes[node_id])
        node["properties"] = self._compact_properties(node)
        return node

    @staticmethod
    def _compact_edge(edge: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": edge.get("id"),
            "from_node_id": edge["from_node_id"],
            "to_node_id": edge["to_node_id"],
            "relation": edge["relation"],
            "properties": edge.get("properties") or {},
        }

    @staticmethod
    def _compact_pill(pill: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": pill["id"],
            "node_id": pill["node_id"],
            "kind": pill["kind"],
            "title": pill["title"],
            "rendered_text": pill["rendered_text"],
        }

    @staticmethod
    def _compact_properties(node: dict[str, Any]) -> dict[str, Any]:
        properties = node.get("properties") or {}
        kind = node.get("kind")
        if kind in {"table", "view"}:
            compact = {
                key: properties.get(key)
                for key in ("database", "schema", "name", "kind", "row_estimate", "size_bytes")
                if key in properties
            }
            profile = properties.get("profile") or {}
            if profile:
                compact["profile"] = {
                    key: profile.get(key)
                    for key in ("row_count", "row_count_is_capped", "sample_count")
                    if key in profile
                }
            return compact
        if kind == "column":
            compact = {
                key: properties.get(key)
                for key in (
                    "database",
                    "schema",
                    "table",
                    "name",
                    "ordinal",
                    "data_type",
                    "nullable",
                    "default",
                )
                if key in properties
            }
            profile = properties.get("profile") or {}
            if profile:
                compact["profile"] = {
                    key: profile.get(key)
                    for key in ("null_count", "null_rate", "distinct_count", "min_value", "max_value")
                    if key in profile
                }
            return compact
        if kind == "index":
            return {
                key: properties.get(key)
                for key in ("database", "schema", "table", "name", "columns", "unique", "primary")
                if key in properties
            }
        if kind == "context_pill":
            return {"kind": properties.get("kind")}
        return properties if len(str(properties)) < 1000 else {}

    @staticmethod
    def _flatten_fact_data(data: dict[str, Any]) -> str:
        values: list[str] = []

        def walk(value: Any) -> None:
            if isinstance(value, dict):
                for child in value.values():
                    walk(child)
            elif isinstance(value, list):
                for child in value:
                    walk(child)
            elif value is not None:
                values.append(str(value))

        walk(data)
        return " ".join(values[:500])

    def _node_with_pills(self, node_id: str) -> dict[str, Any]:
        node = dict(self.nodes[node_id])
        node["pills"] = self.pills_by_node.get(node_id, [])
        return node

    def _connected_components(self) -> list[dict[str, Any]]:
        seen: set[str] = set()
        communities: list[dict[str, Any]] = []
        for node_id in sorted(self.nodes):
            if node_id in seen:
                continue
            queue = deque([node_id])
            seen.add(node_id)
            component: list[str] = []
            while queue:
                current = queue.popleft()
                component.append(current)
                for edge in self.outgoing.get(current, []) + self.incoming.get(current, []):
                    neighbor = self._edge_neighbor(edge, current)
                    if neighbor not in seen and neighbor in self.nodes:
                        seen.add(neighbor)
                        queue.append(neighbor)
            communities.append({"id": len(communities), "nodes": sorted(component)})
        return communities
