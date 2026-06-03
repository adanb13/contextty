from __future__ import annotations

import html
import json
import re
from collections import Counter
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .facts import ROW_FACT_KINDS
from .graph import ContextGraph
from .models import SnapshotRun, Source
from .storage import LocalStore


def report_path_for_source(store: LocalStore, source_name: str) -> Path:
    return store.path.parent / "reports" / f"{safe_report_filename(source_name)}.html"


def write_snapshot_report(
    store: LocalStore,
    source_name: str,
    snapshot_run_id: int | None = None,
) -> Path:
    source = store.get_source(source_name)
    run = store.resolve_snapshot_run(source_id=source.id, snapshot_run_id=snapshot_run_id)
    if run is None or run.status != "success" or run.source_id != source.id:
        raise KeyError(f"successful snapshot artifact not found: {source_name}")

    report_path = report_path_for_source(store, source.name)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(render_snapshot_report(store, source, run), encoding="utf-8")
    return report_path


def render_snapshot_report(store: LocalStore, source: Source, run: SnapshotRun) -> str:
    data = snapshot_report_data(store, source, run)
    title = f"Contextty Report: {source.name}"
    data_json = html.escape(json.dumps(data, sort_keys=True, default=str), quote=False)
    return REPORT_TEMPLATE.format(
        title=html.escape(title),
        source_name=html.escape(source.name),
        connector_type=html.escape(source.connector_type),
        run_id=run.id,
        profile_mode=html.escape(run.profile_mode),
        finished_at=html.escape(run.finished_at or ""),
        data_json=data_json,
    )


def snapshot_report_data(store: LocalStore, source: Source, run: SnapshotRun) -> dict[str, Any]:
    graph = ContextGraph(store, source_id=source.id, snapshot_run_id=run.id)
    nodes = list(graph.nodes.values())
    edges = graph.edges
    pills = graph.pills
    facts = graph.facts
    communities = graph.communities()
    degree_centrality = graph.degree_centrality()
    degree_by_node = {item["node_id"]: item for item in degree_centrality}
    community_by_node: dict[str, int] = {}
    for community in communities:
        for node_id in community.get("nodes", []):
            community_by_node[str(node_id)] = int(community["id"])

    enriched_nodes = []
    for node in nodes:
        centrality = degree_by_node.get(node["id"], {})
        enriched_nodes.append(
            {
                **node,
                "degree": centrality.get("degree", 0),
                "centrality": centrality.get("centrality", 0),
                "community_id": community_by_node.get(node["id"]),
            }
        )

    nodes_by_id = {node["id"]: node for node in enriched_nodes}
    relationships = []
    for edge in edges:
        from_node = nodes_by_id.get(edge["from_node_id"], {})
        to_node = nodes_by_id.get(edge["to_node_id"], {})
        relationships.append(
            {
                **edge,
                "from_name": from_node.get("qualified_name") or edge["from_node_id"],
                "to_name": to_node.get("qualified_name") or edge["to_node_id"],
            }
        )

    central_tables = [
        item
        for item in degree_centrality
        if nodes_by_id.get(item["node_id"], {}).get("kind") in {"table", "view"}
    ][:20]
    row_facts = [fact for fact in facts if fact.get("kind") in ROW_FACT_KINDS]

    return {
        "source": asdict(source),
        "run": asdict(run),
        "metrics": {
            "nodes": len(nodes),
            "edges": len(edges),
            "pills": len(pills),
            "facts": len(facts),
            "row_facts": len(row_facts),
            "node_kinds": dict(sorted(Counter(node["kind"] for node in nodes).items())),
            "edge_relations": dict(sorted(Counter(edge["relation"] for edge in edges).items())),
            "fact_kinds": dict(sorted(Counter(fact["kind"] for fact in facts).items())),
        },
        "graph": {
            "nodes": enriched_nodes,
            "edges": edges,
            "communities": communities,
            "degree_centrality": degree_centrality,
        },
        "central_tables": central_tables,
        "relationships": relationships,
        "communities": communities,
        "pills": pills,
        "facts": facts,
        "row_facts": row_facts,
    }


def safe_report_filename(source_name: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", source_name).strip("._")
    return safe or "source"


REPORT_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f7f8fb;
      --panel: #ffffff;
      --ink: #172033;
      --muted: #607086;
      --line: #d9e0ea;
      --accent: #14736f;
      --accent-2: #a64235;
      --accent-3: #5b6f95;
      --shade: #eef3f7;
      --shadow: 0 8px 24px rgba(23, 32, 51, 0.08);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; background: var(--bg); color: var(--ink); }}
    header {{
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 24px;
      align-items: end;
      padding: 28px 32px 22px;
      border-bottom: 1px solid var(--line);
      background: var(--panel);
    }}
    h1 {{ margin: 0; font-size: 30px; line-height: 1.15; letter-spacing: 0; }}
    h2 {{ margin: 0 0 14px; font-size: 19px; letter-spacing: 0; }}
    h3 {{ margin: 0 0 8px; font-size: 14px; letter-spacing: 0; text-transform: uppercase; color: var(--muted); }}
    p {{ margin: 0; }}
    .subhead {{ margin-top: 8px; color: var(--muted); }}
    .run-meta {{ text-align: right; color: var(--muted); font-size: 14px; }}
    main {{ max-width: 1480px; margin: 0 auto; padding: 24px 28px 40px; }}
    .metrics {{
      display: grid;
      grid-template-columns: repeat(5, minmax(140px, 1fr));
      gap: 12px;
      margin-bottom: 20px;
    }}
    .metric {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 14px;
      box-shadow: var(--shadow);
    }}
    .metric span {{ display: block; color: var(--muted); font-size: 12px; text-transform: uppercase; }}
    .metric strong {{ display: block; margin-top: 4px; font-size: 26px; }}
    .workspace {{
      display: grid;
      grid-template-columns: minmax(0, 1.6fr) minmax(320px, 0.85fr);
      gap: 18px;
      margin-bottom: 22px;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: var(--shadow);
      min-width: 0;
    }}
    .panel-head {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      align-items: center;
      justify-content: space-between;
      padding: 14px;
      border-bottom: 1px solid var(--line);
    }}
    .controls {{ display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }}
    input, select {{
      min-height: 36px;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 7px 10px;
      color: var(--ink);
      background: #fff;
      font: inherit;
    }}
    input {{ width: min(340px, 52vw); }}
    .map-wrap {{ height: 560px; position: relative; }}
    #graph-map {{ display: block; width: 100%; height: 100%; background: linear-gradient(#fff, #f9fbfd); border-radius: 0 0 8px 8px; }}
    .map-edge {{ stroke: #a9b6c8; stroke-opacity: 0.48; stroke-width: 1.2; }}
    .map-node {{ cursor: pointer; stroke: #fff; stroke-width: 2.5; filter: drop-shadow(0 2px 3px rgba(23,32,51,0.18)); }}
    .map-node:hover {{ stroke: #172033; }}
    .map-label {{ pointer-events: none; font-size: 11px; fill: #344055; }}
    .empty {{ color: var(--muted); padding: 16px; }}
    .detail {{ padding: 14px; display: grid; gap: 12px; }}
    .detail-name {{ font-size: 20px; font-weight: 700; overflow-wrap: anywhere; }}
    .detail-meta {{ color: var(--muted); font-size: 13px; overflow-wrap: anywhere; }}
    .tagrow {{ display: flex; flex-wrap: wrap; gap: 6px; }}
    .tag {{ display: inline-flex; align-items: center; min-height: 24px; padding: 3px 8px; border-radius: 999px; background: var(--shade); color: #334056; font-size: 12px; }}
    .list {{ max-height: 260px; overflow: auto; border-top: 1px solid var(--line); }}
    .node-row {{ display: grid; gap: 3px; padding: 9px 12px; border-bottom: 1px solid var(--line); cursor: pointer; }}
    .node-row:hover {{ background: #f3f6fa; }}
    .node-row strong {{ font-size: 13px; overflow-wrap: anywhere; }}
    .node-row span {{ color: var(--muted); font-size: 12px; overflow-wrap: anywhere; }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 18px;
      margin-bottom: 22px;
    }}
    .full {{ grid-column: 1 / -1; }}
    .table-wrap {{ overflow: auto; max-height: 460px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ padding: 9px 10px; border-top: 1px solid var(--line); text-align: left; vertical-align: top; }}
    th {{ color: var(--muted); font-weight: 700; background: #fbfcfe; position: sticky; top: 0; z-index: 1; }}
    td {{ overflow-wrap: anywhere; }}
    .fact-text {{ min-width: 320px; max-width: 780px; }}
    .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace; }}
    @media (max-width: 980px) {{
      header {{ grid-template-columns: 1fr; }}
      .run-meta {{ text-align: left; }}
      .metrics {{ grid-template-columns: repeat(2, minmax(140px, 1fr)); }}
      .workspace, .grid {{ grid-template-columns: 1fr; }}
      .map-wrap {{ height: 440px; }}
    }}
  </style>
</head>
<body>
  <header>
    <div>
      <h1>{source_name}</h1>
      <p class="subhead">Contextty snapshot report for {connector_type}</p>
    </div>
    <div class="run-meta">
      <div>Run #{run_id}</div>
      <div>{profile_mode} profile</div>
      <div>{finished_at}</div>
    </div>
  </header>
  <main>
    <section class="metrics" id="metrics"></section>
    <section class="workspace">
      <div class="panel">
        <div class="panel-head">
          <h2>Artifact Map</h2>
          <div class="controls">
            <input id="node-search" type="search" placeholder="Search nodes">
            <select id="kind-filter" aria-label="Filter by node kind"></select>
          </div>
        </div>
        <div class="map-wrap">
          <svg id="graph-map" role="img" aria-label="Interactive snapshot graph map"></svg>
        </div>
      </div>
      <aside class="panel">
        <div class="panel-head"><h2>Details</h2></div>
        <div class="detail" id="node-detail"></div>
        <div class="list" id="node-list"></div>
      </aside>
    </section>
    <section class="grid">
      <div class="panel">
        <div class="panel-head"><h2>Central Tables</h2></div>
        <div class="table-wrap"><table id="central-tables"></table></div>
      </div>
      <div class="panel">
        <div class="panel-head"><h2>Communities</h2></div>
        <div class="table-wrap"><table id="communities"></table></div>
      </div>
      <div class="panel full">
        <div class="panel-head"><h2>Relationships</h2></div>
        <div class="table-wrap"><table id="relationships"></table></div>
      </div>
      <div class="panel full">
        <div class="panel-head"><h2>Pills</h2></div>
        <div class="table-wrap"><table id="pills"></table></div>
      </div>
      <div class="panel full">
        <div class="panel-head"><h2>Row-Derived Facts</h2></div>
        <div class="table-wrap"><table id="row-facts"></table></div>
      </div>
    </section>
  </main>
  <script type="application/json" id="contextty-report-data">{data_json}</script>
  <script>
    const data = JSON.parse(document.getElementById("contextty-report-data").textContent);
    const nodeById = new Map(data.graph.nodes.map((node) => [node.id, node]));
    const state = {{ selectedNodeId: data.graph.nodes[0]?.id || null }};
    const colors = {{
      database: "#14736f",
      schema: "#5b6f95",
      table: "#a64235",
      view: "#8a5a18",
      column: "#456b3f",
      index: "#7c5798",
      context_pill: "#6a7380"
    }};

    function text(value) {{
      return value === null || value === undefined || value === "" ? "-" : String(value);
    }}

    function appendText(parent, tag, value, className) {{
      const element = document.createElement(tag);
      if (className) element.className = className;
      element.textContent = text(value);
      parent.appendChild(element);
      return element;
    }}

    function renderMetrics() {{
      const metrics = [
        ["Nodes", data.metrics.nodes],
        ["Edges", data.metrics.edges],
        ["Pills", data.metrics.pills],
        ["Facts", data.metrics.facts],
        ["Row Facts", data.metrics.row_facts]
      ];
      const root = document.getElementById("metrics");
      root.replaceChildren();
      for (const [label, value] of metrics) {{
        const card = document.createElement("div");
        card.className = "metric";
        appendText(card, "span", label);
        appendText(card, "strong", value);
        root.appendChild(card);
      }}
    }}

    function configureFilters() {{
      const select = document.getElementById("kind-filter");
      const kinds = [...new Set(data.graph.nodes.map((node) => node.kind))].sort();
      select.replaceChildren();
      const all = document.createElement("option");
      all.value = "";
      all.textContent = "All node kinds";
      select.appendChild(all);
      for (const kind of kinds) {{
        const option = document.createElement("option");
        option.value = kind;
        option.textContent = kind;
        select.appendChild(option);
      }}
      document.getElementById("node-search").addEventListener("input", renderInteractive);
      select.addEventListener("change", renderInteractive);
    }}

    function filteredNodes() {{
      const query = document.getElementById("node-search").value.trim().toLowerCase();
      const kind = document.getElementById("kind-filter").value;
      return data.graph.nodes.filter((node) => {{
        if (kind && node.kind !== kind) return false;
        if (!query) return true;
        return [node.name, node.qualified_name, node.kind, node.summary]
          .filter(Boolean)
          .join(" ")
          .toLowerCase()
          .includes(query);
      }});
    }}

    function renderInteractive() {{
      renderMap();
      renderNodeList();
      renderNodeDetail();
    }}

    function renderMap() {{
      const svg = document.getElementById("graph-map");
      svg.replaceChildren();
      const nodes = filteredNodes();
      const visible = new Set(nodes.map((node) => node.id));
      const width = 1000;
      const height = 620;
      svg.setAttribute("viewBox", `0 0 ${{width}} ${{height}}`);
      if (!nodes.length) {{
        const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
        label.setAttribute("x", width / 2);
        label.setAttribute("y", height / 2);
        label.setAttribute("text-anchor", "middle");
        label.setAttribute("class", "map-label");
        label.textContent = "No matching nodes";
        svg.appendChild(label);
        return;
      }}

      const positions = new Map();
      nodes.forEach((node, index) => {{
        const angle = (Math.PI * 2 * index) / Math.max(1, nodes.length);
        const communityOffset = (Number(node.community_id || 0) % 5) * 18;
        const radius = 180 + communityOffset + Math.min(120, Number(node.degree || 0) * 8);
        positions.set(node.id, {{
          x: width / 2 + Math.cos(angle) * radius,
          y: height / 2 + Math.sin(angle) * radius
        }});
      }});

      for (const edge of data.graph.edges) {{
        if (!visible.has(edge.from_node_id) || !visible.has(edge.to_node_id)) continue;
        const from = positions.get(edge.from_node_id);
        const to = positions.get(edge.to_node_id);
        const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
        line.setAttribute("x1", from.x);
        line.setAttribute("y1", from.y);
        line.setAttribute("x2", to.x);
        line.setAttribute("y2", to.y);
        line.setAttribute("class", "map-edge");
        svg.appendChild(line);
      }}

      for (const node of nodes) {{
        const pos = positions.get(node.id);
        const circle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
        circle.setAttribute("cx", pos.x);
        circle.setAttribute("cy", pos.y);
        circle.setAttribute("r", Math.max(8, Math.min(22, 8 + Number(node.degree || 0) * 1.5)));
        circle.setAttribute("fill", colors[node.kind] || "#4f6678");
        circle.setAttribute("class", "map-node");
        circle.addEventListener("click", () => {{
          state.selectedNodeId = node.id;
          renderInteractive();
        }});
        svg.appendChild(circle);

        const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
        label.setAttribute("x", pos.x + 12);
        label.setAttribute("y", pos.y + 4);
        label.setAttribute("class", "map-label");
        label.textContent = node.name || node.qualified_name || node.id;
        svg.appendChild(label);
      }}
    }}

    function renderNodeList() {{
      const list = document.getElementById("node-list");
      list.replaceChildren();
      for (const node of filteredNodes().slice(0, 250)) {{
        const row = document.createElement("div");
        row.className = "node-row";
        row.addEventListener("click", () => {{
          state.selectedNodeId = node.id;
          renderInteractive();
        }});
        appendText(row, "strong", node.qualified_name || node.name || node.id);
        appendText(row, "span", `${{node.kind}} - degree ${{node.degree || 0}}`);
        list.appendChild(row);
      }}
    }}

    function renderNodeDetail() {{
      const root = document.getElementById("node-detail");
      root.replaceChildren();
      const node = nodeById.get(state.selectedNodeId) || filteredNodes()[0];
      if (!node) {{
        appendText(root, "p", "Select a node from the map or list.", "empty");
        return;
      }}
      appendText(root, "div", node.qualified_name || node.name || node.id, "detail-name");
      appendText(root, "div", node.id, "detail-meta mono");
      const tags = document.createElement("div");
      tags.className = "tagrow";
      for (const value of [node.kind, `degree ${{node.degree || 0}}`, node.community_id !== null && node.community_id !== undefined ? `community ${{node.community_id}}` : null]) {{
        if (!value) continue;
        appendText(tags, "span", value, "tag");
      }}
      root.appendChild(tags);
      if (node.summary) appendText(root, "p", node.summary);

      const relatedPills = data.pills.filter((pill) => pill.node_id === node.id).slice(0, 6);
      if (relatedPills.length) {{
        appendText(root, "h3", "Pills");
        for (const pill of relatedPills) appendText(root, "p", `${{pill.kind}}: ${{pill.rendered_text}}`);
      }}
      const relatedFacts = data.facts.filter((fact) => fact.node_id === node.id).slice(0, 6);
      if (relatedFacts.length) {{
        appendText(root, "h3", "Facts");
        for (const fact of relatedFacts) appendText(root, "p", `${{fact.kind}}: ${{fact.text}}`);
      }}
    }}

    function renderTable(id, columns, rows, emptyText = "No rows") {{
      const table = document.getElementById(id);
      table.replaceChildren();
      const thead = document.createElement("thead");
      const header = document.createElement("tr");
      for (const column of columns) appendText(header, "th", column.label);
      thead.appendChild(header);
      table.appendChild(thead);
      const tbody = document.createElement("tbody");
      if (!rows.length) {{
        const row = document.createElement("tr");
        const cell = appendText(row, "td", emptyText);
        cell.colSpan = columns.length;
        tbody.appendChild(row);
      }} else {{
        for (const item of rows) {{
          const row = document.createElement("tr");
          for (const column of columns) {{
            const cell = appendText(row, "td", column.value(item));
            if (column.className) cell.className = column.className;
          }}
          tbody.appendChild(row);
        }}
      }}
      table.appendChild(tbody);
    }}

    function renderStaticTables() {{
      renderTable("central-tables", [
        {{ label: "Table", value: (row) => row.qualified_name }},
        {{ label: "Kind", value: (row) => row.kind }},
        {{ label: "Degree", value: (row) => row.degree }},
        {{ label: "Centrality", value: (row) => Number(row.centrality || 0).toFixed(3) }}
      ], data.central_tables);

      renderTable("communities", [
        {{ label: "Community", value: (row) => row.id }},
        {{ label: "Nodes", value: (row) => row.nodes.length }},
        {{ label: "Members", value: (row) => row.nodes.slice(0, 24).map((id) => nodeById.get(id)?.qualified_name || id).join(", "), className: "fact-text" }}
      ], data.communities);

      renderTable("relationships", [
        {{ label: "From", value: (row) => row.from_name }},
        {{ label: "Relation", value: (row) => row.relation }},
        {{ label: "To", value: (row) => row.to_name }}
      ], data.relationships);

      renderTable("pills", [
        {{ label: "Kind", value: (row) => row.kind }},
        {{ label: "Title", value: (row) => row.title }},
        {{ label: "Text", value: (row) => row.rendered_text, className: "fact-text" }}
      ], data.pills);

      renderTable("row-facts", [
        {{ label: "Kind", value: (row) => row.kind }},
        {{ label: "Subject", value: (row) => row.subject }},
        {{ label: "Text", value: (row) => row.text, className: "fact-text" }}
      ], data.row_facts, "No row-derived facts were captured for this snapshot");
    }}

    renderMetrics();
    configureFilters();
    renderInteractive();
    renderStaticTables();
  </script>
</body>
</html>
"""
