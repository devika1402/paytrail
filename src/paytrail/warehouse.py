"""Serverless SQL warehouse access for paytrail.

One place to run a statement against the Databricks serverless SQL warehouse and
get its rows back, failing loud on any non-SUCCEEDED state. Auth is the ambient
Databricks CLI profile (OAuth U2M, profile ``paytrail``), so no secret is handled
here. Reused by bronze ingest, gold setup, and the benchmark.
"""

from __future__ import annotations

import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState

# Warehouse id is read from the DATABRICKS_WAREHOUSE_ID env var (see resolve_warehouse_id).
DEFAULT_WAREHOUSE_ID = ""

# COPY INTO / statement waits: the warehouse auto-starts from cold, so allow the
# SDK's maximum inline wait before it falls back to async polling.
_WAIT_TIMEOUT = "50s"


def resolve_warehouse_id(explicit: str | None = None) -> str:
    """Return the warehouse id to use: explicit arg, else env, else the default."""
    return explicit or os.environ.get("DATABRICKS_WAREHOUSE_ID") or DEFAULT_WAREHOUSE_ID


def execute(
    statement: str,
    warehouse_id: str | None = None,
    client: WorkspaceClient | None = None,
    parameters: list[StatementParameterListItem] | None = None,
) -> list[list[str]]:
    """Run one SQL statement; return its result rows (each a list of string cells).

    Polls to completion when the warehouse cannot finish within the inline wait
    (a cold warehouse start), so callers never see a PENDING result.

    Args:
        parameters: optional bound parameters for a parameterised statement. Pass any
            caller-supplied value this way (marker ``:name`` in the SQL) rather than
            interpolating it into the string, so a value containing a quote can neither
            break nor inject into the statement.

    Raises:
        RuntimeError: if the statement ends in any state other than SUCCEEDED,
            carrying the warehouse's error message and the statement preview.
    """
    client = client or WorkspaceClient()
    warehouse_id = resolve_warehouse_id(warehouse_id)
    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout=_WAIT_TIMEOUT,
        parameters=parameters,
    )
    statement_id = response.statement_id
    state = response.status.state if response.status is not None else None

    # Cold-start / long statement: poll until it leaves the running states.
    while state in (StatementState.PENDING, StatementState.RUNNING) and statement_id:
        response = client.statement_execution.get_statement(statement_id)
        state = response.status.state if response.status is not None else None

    if state is not StatementState.SUCCEEDED:
        error = response.status.error if response.status is not None else None
        detail = error.message if error is not None else "no error detail returned"
        preview = statement.strip().splitlines()[0][:80]
        raise RuntimeError(
            f"Statement did not succeed (state={state}): {preview!r} -> {detail}"
        )

    result = response.result
    if result is None or result.data_array is None:
        return []
    return result.data_array


def scalar(
    statement: str,
    warehouse_id: str | None = None,
    client: WorkspaceClient | None = None,
    parameters: list[StatementParameterListItem] | None = None,
) -> str | None:
    """Run a statement expected to return a single cell; return it (or None).

    ``parameters`` forwards bound parameters to :func:`execute` (see its docstring);
    use it for any caller-supplied value instead of interpolating into the SQL.
    """
    rows = execute(
        statement, warehouse_id=warehouse_id, client=client, parameters=parameters
    )
    if not rows or not rows[0]:
        return None
    return rows[0][0]


def execute_dicts(
    statement: str,
    warehouse_id: str | None = None,
    client: WorkspaceClient | None = None,
) -> list[dict[str, str]]:
    """Run a statement and return rows as {column_name: value} dicts.

    Reads column names from the result manifest, so callers (e.g. parsing
    ``DESCRIBE DETAIL``) don't depend on positional column order.
    """
    client = client or WorkspaceClient()
    warehouse_id = resolve_warehouse_id(warehouse_id)
    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout=_WAIT_TIMEOUT,
    )
    statement_id = response.statement_id
    state = response.status.state if response.status is not None else None
    while state in (StatementState.PENDING, StatementState.RUNNING) and statement_id:
        response = client.statement_execution.get_statement(statement_id)
        state = response.status.state if response.status is not None else None
    if state is not StatementState.SUCCEEDED:
        error = response.status.error if response.status is not None else None
        detail = error.message if error is not None else "no error detail returned"
        preview = statement.strip().splitlines()[0][:80]
        raise RuntimeError(f"Statement did not succeed (state={state}): {preview!r} -> {detail}")
    manifest = response.manifest
    schema = manifest.schema if manifest is not None else None
    columns = [c.name or "" for c in (schema.columns or [])] if schema is not None else []
    result = response.result
    rows = result.data_array if result is not None and result.data_array is not None else []
    return [dict(zip(columns, row, strict=False)) for row in rows]
