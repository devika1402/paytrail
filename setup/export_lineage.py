"""Export Unity Catalog table lineage (bronze → silver → gold) to docs/lineage/.

UC captures lineage automatically from executed queries; this reads it back via the
lineage-tracking REST API for each pipeline table and writes:
  * docs/lineage/lineage.json, the raw upstream edges per table
  * docs/lineage/lineage.md, a readable layered graph (the flow a reviewer sees)

This is the governance payoff of Unity Catalog: any gold number traces back to the
raw bronze row it came from, without instrumenting the pipeline ourselves.

Usage:
    python setup/export_lineage.py
"""

from __future__ import annotations

import json
from pathlib import Path

from databricks.sdk import WorkspaceClient

REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = REPO_ROOT / "docs" / "lineage"

# Pipeline tables, in medallion order, to resolve lineage for.
TABLES = [
    "paytrail.bronze.transactions_raw",
    "paytrail.silver.stg_transactions",
    "paytrail.silver.transactions",
    "paytrail.silver.transactions_quarantine",
    "paytrail.gold.fact_transaction",
    "paytrail.gold.dim_account",
    "paytrail.gold.dim_customer",
    "paytrail.gold.dim_date",
    "paytrail.gold.dim_transaction_type",
    "paytrail.gold.mart_daily_settled_volume",
]


def upstreams_of(client: WorkspaceClient, table: str) -> list[str]:
    """Return the fully-qualified upstream tables feeding ``table`` (UC lineage API)."""
    response = client.api_client.do(
        "GET",
        "/api/2.0/lineage-tracking/table-lineage",
        body={"table_name": table, "include_entity_lineage": False},
    )
    edges: list[str] = []
    for entry in response.get("upstreams", []) if isinstance(response, dict) else []:
        info = entry.get("tableInfo")
        if not info:
            continue  # non-table upstream (e.g. a path), skip
        name = f"{info.get('catalog_name')}.{info.get('schema_name')}.{info.get('name')}"
        if info.get("catalog_name") == "paytrail":
            edges.append(name)
    return sorted(set(edges))


def layer_of(table: str) -> str:
    """Return the medallion layer (schema) of a fully-qualified table name."""
    return table.split(".")[1]


def build_graph(client: WorkspaceClient) -> dict[str, list[str]]:
    """Resolve upstream edges for every pipeline table."""
    graph: dict[str, list[str]] = {}
    for table in TABLES:
        graph[table] = upstreams_of(client, table)
        print(f"[lineage] {table} <- {graph[table] or '(source)'}")
    return graph


def render_markdown(graph: dict[str, list[str]]) -> str:
    """Render the lineage as a readable, layered Markdown document."""
    lines = [
        "# Unity Catalog lineage, bronze → silver → gold",
        "",
        "Captured automatically by Unity Catalog from executed queries, exported via the",
        "lineage-tracking API (`setup/export_lineage.py`). Every gold figure traces back",
        "to the raw bronze row it derives from.",
        "",
        "## Edges (upstream → table)",
        "",
    ]
    for table in TABLES:
        ups = graph.get(table, [])
        if ups:
            for up in ups:
                lines.append(f"- `{up}` → `{table}`")
        elif table == "paytrail.bronze.transactions_raw":
            lines.append(f"- `{table}` _(source, lands from ADLS Gen2 via bronze ingest)_")
        else:
            lines.append(f"- `{table}` _(static, literal VALUES, no upstream)_")
    lines += ["", "## Flow", "", "```"]
    for layer in ("bronze", "silver", "gold"):
        members = [t for t in TABLES if layer_of(t) == layer]
        lines.append(f"{layer}: " + ", ".join(t.split('.')[-1] for t in members))
    lines += ["```", ""]
    return "\n".join(lines)


def main() -> int:
    """Resolve lineage for all pipeline tables and write the artifacts."""
    client = WorkspaceClient()
    graph = build_graph(client)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "lineage.json").write_text(json.dumps(graph, indent=2) + "\n", encoding="utf-8")
    (OUT_DIR / "lineage.md").write_text(render_markdown(graph), encoding="utf-8")
    print(f"[lineage] wrote {OUT_DIR}/lineage.json and lineage.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
