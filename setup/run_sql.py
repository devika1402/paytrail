"""Execute a ``.sql`` file statement-by-statement against a Databricks SQL warehouse.

Code-first alternative to clicking DDL into the workspace UI. Used to create
the Unity Catalog structure (``setup/uc_setup.sql``); reusable for any idempotent
setup SQL. Auth is the ambient Databricks CLI profile (OAuth U2M), so no secret is
passed here.

Usage:
    python setup/run_sql.py setup/uc_setup.sql --warehouse-id <id>
    # or set DATABRICKS_WAREHOUSE_ID instead of --warehouse-id
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


def split_statements(sql_text: str) -> list[str]:
    """Split a SQL script into individual statements.

    Strips ``--`` line comments and splits on ``;``. This assumes no semicolons appear
    inside statement bodies or string literals (the paytrail setup SQL is written to
    honour that), which keeps the runner free of a heavyweight SQL parser.
    """
    lines = [
        line for line in sql_text.splitlines() if not line.strip().startswith("--")
    ]
    stripped = "\n".join(lines)
    return [stmt.strip() for stmt in stripped.split(";") if stmt.strip()]


def execute_statement(client: WorkspaceClient, warehouse_id: str, statement: str) -> None:
    """Run one SQL statement and fail loud if it does not reach SUCCEEDED.

    Raises:
        RuntimeError: if the statement ends in any state other than SUCCEEDED,
            including the error message returned by the warehouse.
    """
    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="50s",
    )
    status = response.status
    state = status.state if status is not None else None
    if state is not StatementState.SUCCEEDED:
        error = status.error if status is not None else None
        detail = error.message if error is not None else "no error detail returned"
        preview = statement.splitlines()[0][:80]
        raise RuntimeError(
            f"Statement did not succeed (state={state}): {preview!r} -> {detail}"
        )


def run_sql_file(sql_path: Path, warehouse_id: str) -> int:
    """Execute every statement in ``sql_path``; return the count executed."""
    if not sql_path.is_file():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")
    statements = split_statements(sql_path.read_text(encoding="utf-8"))
    if not statements:
        raise ValueError(f"No executable statements found in {sql_path}")

    client = WorkspaceClient()
    for index, statement in enumerate(statements, start=1):
        preview = statement.splitlines()[0][:80]
        print(f"[{index}/{len(statements)}] {preview}")
        execute_statement(client, warehouse_id, statement)
    return len(statements)


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sql_file", type=Path, help="Path to the .sql file to execute")
    parser.add_argument(
        "--warehouse-id",
        default=os.environ.get("DATABRICKS_WAREHOUSE_ID"),
        help="SQL warehouse id (defaults to $DATABRICKS_WAREHOUSE_ID)",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    """CLI entry point."""
    args = parse_args(argv)
    if not args.warehouse_id:
        raise SystemExit(
            "warehouse id required: pass --warehouse-id or set DATABRICKS_WAREHOUSE_ID"
        )
    count = run_sql_file(args.sql_file, args.warehouse_id)
    print(f"OK: executed {count} statement(s) against warehouse {args.warehouse_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
