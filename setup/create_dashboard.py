"""Create the AI/BI (Lakeview) dashboard on the gold mart, code-first.

Builds a Databricks AI/BI dashboard on `paytrail.gold.mart_daily_settled_volume`.
The chart forms are chosen for the data, which spans a ~2,400x range across
transaction types (TRANSFER ~485bn vs DEBIT ~0.2bn), so overlapping lines on a shared
linear axis are unreadable. Instead:

- KPI counters: total transactions, total settled volume, and the overall fraud RATE
  (a rate carries more than a raw count, and fraud is concentrated in two types);
- one clean total-volume trend line (summed across types) for change over time;
- four ranked, value-labelled horizontal bars that profile each transaction type by a
  different metric (volume, count, average ticket size, fraud rate), so magnitude
  survives the range and the type-level story is comparable at a glance;
- one settled-volume-by-segment bar for the PM view; two filters drive the page.

Schema note: datasets use a `query` string (Databricks stores it as `queryLines`);
widget queries follow the exported AI/BI format (`main_query` -> datasetName + fields
+ encodings), which is what the editor binds against.

Usage:
    python setup/create_dashboard.py
"""

from __future__ import annotations

import json
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import dashboards

from paytrail import warehouse

REPO_ROOT = Path(__file__).resolve().parents[1]
DEF_PATH = REPO_ROOT / "bi" / "paytrail_mart.lvdash.json"
MART = "paytrail.gold.mart_daily_settled_volume"
DATASET = "mart"
DISPLAY_NAME = "paytrail: daily settled volume"

# Measure fields (name is the id the encodings reference; expression is the SQL).
M_VOLUME = {"name": "sum(settled_amount)", "expression": "SUM(`settled_amount`)"}
M_COUNT = {"name": "sum(transaction_count)", "expression": "SUM(`transaction_count`)"}
M_AVG_TICKET = {"name": "avg_ticket",
                "expression": "SUM(`settled_amount`) / SUM(`transaction_count`)"}
# Aggregate-first (SUM ... * 100): a LEADING scalar literal makes the AI/BI measure
# parser reject the field, so the * 100 must trail the aggregates.
M_FRAUD_RATE = {"name": "fraud_rate_pct",
                "expression": "SUM(`fraud_count`) / SUM(`transaction_count`) * 100"}


def _dim(col: str) -> dict[str, str]:
    return {"name": col, "expression": f"`{col}`"}


def _query(fields: list[dict[str, str]], disaggregated: bool = False) -> dict[str, object]:
    return {
        "name": "main_query",
        "query": {"datasetName": DATASET, "fields": fields, "disaggregated": disaggregated},
    }


def _counter(name: str, measure: dict[str, str], title: str, x: int, y: int) -> dict[str, object]:
    return {
        "widget": {
            "name": name,
            "queries": [_query([measure])],
            "spec": {"version": 2, "widgetType": "counter",
                     "encodings": {"value": {"fieldName": measure["name"], "displayName": title}},
                     "frame": {"showTitle": True, "title": title}},
        },
        "position": {"x": x, "y": y, "width": 4, "height": 3},
    }


def _ranked_hbar(name: str, dim: str, measure: dict[str, str], title: str,
                 axis: str, x: int, y: int, w: int, h: int) -> dict[str, object]:
    """Horizontal bar ranked by the measure, with value labels; reads despite big range.

    Axis labels are short (``axis``); the full description lives in the frame title, so
    the rotated y-axis caption never overruns a narrow widget.
    """
    return {
        "widget": {
            "name": name,
            "queries": [_query([_dim(dim), measure])],
            "spec": {
                "version": 3, "widgetType": "bar",
                "encodings": {
                    "x": {"fieldName": measure["name"], "scale": {"type": "quantitative"},
                          "displayName": axis},
                    "y": {"fieldName": dim,
                          "scale": {"type": "categorical", "sort": {"by": "x-reversed"}},
                          "displayName": "type"},
                    # Colour each bar by its type, so a type keeps the SAME colour across
                    # all four type charts (consistent identity, not just decoration).
                    "color": {"fieldName": dim, "scale": {"type": "categorical"},
                              "legend": {"hide": True}, "displayName": "type"},
                    "label": {"show": True},
                },
                "frame": {"showTitle": True, "title": title},
            },
        },
        "position": {"x": x, "y": y, "width": w, "height": h},
    }


def _trend_line(name: str, measure: dict[str, str], title: str,
                x: int, y: int, w: int, h: int) -> dict[str, object]:
    """Single-series line over date (no color): one clean trend reads at a glance."""
    return {
        "widget": {
            "name": name,
            "queries": [_query([_dim("date_key"), measure])],
            "spec": {
                "version": 3, "widgetType": "line",
                "encodings": {
                    "x": {"fieldName": "date_key", "scale": {"type": "temporal"},
                          "displayName": "Date"},
                    "y": {"fieldName": measure["name"], "scale": {"type": "quantitative"},
                          "displayName": "Settled volume"},
                },
                "frame": {"showTitle": True, "title": title},
            },
        },
        "position": {"x": x, "y": y, "width": w, "height": h},
    }


def _segment_bar(name: str, measure: dict[str, str], title: str,
                 x: int, y: int, w: int, h: int) -> dict[str, object]:
    """Vertical bar by customer segment (3 readable bars) for the PM view."""
    return {
        "widget": {
            "name": name,
            "queries": [_query([_dim("customer_segment"), measure])],
            "spec": {
                "version": 3, "widgetType": "bar",
                "encodings": {
                    "x": {"fieldName": "customer_segment", "scale": {"type": "categorical"},
                          "displayName": "Segment"},
                    "y": {"fieldName": measure["name"], "scale": {"type": "quantitative"},
                          "displayName": "Volume"},
                    "color": {"fieldName": "customer_segment", "scale": {"type": "categorical"},
                              "displayName": "Segment"},
                    "label": {"show": True},
                },
                "frame": {"showTitle": True, "title": title},
            },
        },
        "position": {"x": x, "y": y, "width": w, "height": h},
    }


def _filter(name: str, col: str, title: str, x: int) -> dict[str, object]:
    """A page filter. Placed at the TOP (y=0): controls read before the charts they drive."""
    return {
        "widget": {
            "name": name,
            "queries": [_query([_dim(col)], disaggregated=True)],
            "spec": {"version": 2, "widgetType": "filter-multi-select",
                     "encodings": {"fields": [{"fieldName": col, "displayName": title,
                                               "queryName": "main_query"}]},
                     "frame": {"showTitle": True, "title": title}},
        },
        "position": {"x": x, "y": 0, "width": 3, "height": 2},
    }


def build_definition() -> dict[str, object]:
    """The serialized AI/BI dashboard: KPIs + trend + type-profile bars + segment + filters."""
    query = (
        "SELECT date_key, transaction_type, customer_segment,\n"
        "       settled_amount, transaction_count, fraud_count\n"
        f"FROM {MART}"
    )
    layout = [
        # Filters first: at the top (y=0), where controls belong.
        _filter("filter_type", "transaction_type", "Transaction type", 0),
        _filter("filter_segment", "customer_segment", "Customer segment", 3),
        # KPI row.
        _counter("kpi_txns", M_COUNT, "Total transactions", 0, 2),
        _counter("kpi_volume", M_VOLUME, "Total settled volume", 4, 2),
        _counter("kpi_fraud", M_FRAUD_RATE, "Overall fraud rate (%)", 8, 2),
        # Trend + segment.
        _trend_line("trend_total", M_VOLUME, "Total settled volume over time", 0, 5, 8, 6),
        _segment_bar("vol_by_segment", M_VOLUME, "Settled volume by customer segment", 8, 5, 4, 6),
        # Type profile: four ranked bars, one metric each.
        _ranked_hbar("vol_by_type", "transaction_type", M_VOLUME,
                     "Settled volume by type", "Volume", 0, 11, 6, 6),
        _ranked_hbar("count_by_type", "transaction_type", M_COUNT,
                     "Transaction count by type", "Transactions", 6, 11, 6, 6),
        _ranked_hbar("avgticket_by_type", "transaction_type", M_AVG_TICKET,
                     "Average ticket size by type", "Avg ticket", 0, 17, 6, 6),
        _ranked_hbar("fraud_by_type", "transaction_type", M_FRAUD_RATE,
                     "Fraud rate by type (%)", "Fraud rate %", 6, 17, 6, 6),
    ]
    return {
        "datasets": [{"name": DATASET, "displayName": "Daily settled volume", "query": query}],
        "pages": [{"name": "overview", "displayName": "Overview",
                   "pageType": "PAGE_TYPE_CANVAS", "layout": layout}],
    }


def main() -> int:
    """Write the definition and deploy + publish the dashboard (idempotent)."""
    definition = build_definition()
    DEF_PATH.parent.mkdir(parents=True, exist_ok=True)
    DEF_PATH.write_text(json.dumps(definition, indent=2) + "\n", encoding="utf-8")
    print(f"[dashboard] wrote definition -> {DEF_PATH}")

    client = WorkspaceClient()
    wh = warehouse.resolve_warehouse_id()
    me = client.current_user.me().user_name
    serialized = json.dumps(definition)

    active = dashboards.LifecycleState.ACTIVE
    existing = next(
        (d for d in client.lakeview.list()
         if d.display_name == DISPLAY_NAME and d.lifecycle_state == active),
        None,
    )
    if existing is not None and existing.dashboard_id is not None:
        client.lakeview.update(
            existing.dashboard_id,
            dashboards.Dashboard(display_name=DISPLAY_NAME, warehouse_id=wh,
                                 serialized_dashboard=serialized),
        )
        dashboard_id = existing.dashboard_id
        print(f"[dashboard] updated existing: {dashboard_id}")
    else:
        created = client.lakeview.create(
            dashboards.Dashboard(
                display_name=DISPLAY_NAME, warehouse_id=wh,
                parent_path=f"/Users/{me}", serialized_dashboard=serialized,
            )
        )
        if created.dashboard_id is None:
            raise RuntimeError("Dashboard creation returned no id")
        dashboard_id = created.dashboard_id
        print(f"[dashboard] created: {dashboard_id} at {created.path}")

    published = client.lakeview.publish(dashboard_id, warehouse_id=wh)
    print(f"[dashboard] published (embed_credentials={published.embed_credentials}). "
          f"Open it in the workspace and screenshot into docs/dashboards/.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
