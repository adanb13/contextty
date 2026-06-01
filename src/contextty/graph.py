from __future__ import annotations

import math
import re
from collections import deque
from typing import Any

from .storage import LocalStore


def tokenize(text: str) -> set[str]:
    tokens: set[str] = set()
    for token in re.findall(r"[A-Za-z0-9_]+", text.lower()):
        if len(token) > 1:
            tokens.add(token)
        for part in token.split("_"):
            if len(part) > 1:
                tokens.add(part)
    return tokens


class ContextGraph:
    def __init__(self, store: LocalStore, source_id: int | None = None, snapshot_run_id: int | None = None) -> None:
        self.store = store
        self.nodes = {node["id"]: node for node in store.get_nodes(source_id=source_id, snapshot_run_id=snapshot_run_id)}
        self.edges = store.get_edges(source_id=source_id, snapshot_run_id=snapshot_run_id)
        self.pills = store.get_pills(source_id=source_id, snapshot_run_id=snapshot_run_id)
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
        seeds = self._rank_nodes(query)[:5]
        seed_ids = [node["id"] for node, _score in seeds]
        if not seed_ids and self.nodes:
            seed_ids = list(self.nodes)[:1]
        node_ids, edges = self._traverse(seed_ids, hops=hops, direction=direction)
        ranked = sorted(
            (self.nodes[node_id] for node_id in node_ids if node_id in self.nodes),
            key=lambda node: (-self._node_score(query, node), node["kind"], node["qualified_name"]),
        )
        selected_nodes, selected_pills, context = self._render_context(ranked, budget)
        return {
            "query": query,
            "seeds": [self._node_with_pills(node_id) for node_id in seed_ids if node_id in self.nodes],
            "nodes": selected_nodes,
            "edges": [
                edge for edge in edges if edge["from_node_id"] in {node["id"] for node in selected_nodes}
                or edge["to_node_id"] in {node["id"] for node in selected_nodes}
            ],
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
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
        selected_nodes: list[dict[str, Any]] = []
        selected_pills: list[dict[str, Any]] = []
        lines: list[str] = []
        words = 0
        max_words = max(1, budget)
        for node in ranked_nodes:
            node_lines: list[str] = []
            if node.get("summary"):
                node_lines.append(node["summary"])
            pills = self.pills_by_node.get(node["id"], [])
            node_lines.extend(pill["rendered_text"] for pill in pills)
            text = "\n".join(line for line in node_lines if line)
            if not text:
                continue
            text_words = len(text.split())
            if lines and words + text_words > max_words:
                break
            lines.append(text)
            words += text_words
            selected_nodes.append(self._node_with_pills(node["id"]))
            selected_pills.extend(pills)
        return selected_nodes, selected_pills, "\n\n".join(lines)

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
