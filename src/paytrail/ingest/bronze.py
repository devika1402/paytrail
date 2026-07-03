"""Bronze ingest, land the PaySim source untouched, append-only, with metadata.

Bronze's source of truth is the **Azure ADLS Gen2** container. This module
reads from there and writes the managed Delta table
``paytrail.bronze.transactions_raw``. Nothing is cleaned or retyped, every source
column is landed as ``STRING`` (silver does the typing). Two ingest-metadata
columns are added: ``_source_file`` and ``_load_ts``.

The write path (the real risk on Free Edition, no local Spark you control):
ADLS read → Parquet → staged into a **Unity Catalog Volume** via the Files API →
``COPY INTO`` on the serverless warehouse. This was de-risked before it was
written; ``COPY INTO`` also gives file-level idempotency for free. On top of that,
ingest is keyed on ``_source_file`` so a re-run of the same source is a no-op
(a payment-pipeline correctness property, not a nicety).

Iterate on the 10k smoke sample (``raw/paysim_sample_10k.csv``); run the full
6.3M-row file (``raw/paysim.csv``) once, last, to protect the Free Edition quota.
"""

from __future__ import annotations

import argparse
import io
import re
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from azure.storage.blob import ContainerClient
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem

from paytrail import warehouse
from paytrail.azure import adls

TABLE = "paytrail.bronze.transactions_raw"
VOLUME = "paytrail.bronze.landing"
VOLUME_PATH = "/Volumes/paytrail/bronze/landing"

DEFAULT_BLOB = "raw/paysim.csv"
SMOKE_BLOB = "raw/paysim_sample_10k.csv"

SOURCE_FILE_COL = "_source_file"
LOAD_TS_COL = "_load_ts"
# Account-identifier columns the UC column mask is bound to (docs/GOVERNANCE.md).
MASKED_COLUMNS = ("nameOrig", "nameDest")
MASK_FUNCTION = "paytrail.bronze.mask_account"

# A source/blob name is interpolated into a Volume path and a COPY INTO string
# literal, so it must be a safe path token: letters, digits, and ._/- only.
# fullmatch (not $) so a trailing newline cannot slip through the anchor.
_SAFE_SOURCE_NAME = re.compile(r"[A-Za-z0-9._/-]+")


def _validate_source_name(source_file: str) -> None:
    """Reject a source name that is not a safe relative path token before interpolation.

    The name flows into a filesystem path (the landing Volume subdir) and into the
    ``COPY INTO`` string literal. Constraining it to ``[A-Za-z0-9._/-]`` (whole string),
    with no ``..`` segment and no leading ``/``, means a crafted name can neither escape
    the landing directory (path traversal) nor break out of the SQL string, closing the
    one interpolation the value-binding on the count query cannot cover.
    """
    if (
        not source_file
        or not _SAFE_SOURCE_NAME.fullmatch(source_file)
        or ".." in source_file
        or source_file.startswith("/")
    ):
        raise ValueError(
            f"Unsafe source name {source_file!r}: expected a relative path of only "
            f"letters, digits, and ._/- with no '..' segment (for example "
            f"'raw/paysim.csv')."
        )


def read_source(blob_name: str, container: ContainerClient | None = None) -> pd.DataFrame:
    """Read a source CSV from ADLS Gen2 into a DataFrame, all columns as strings.

    Streams the blob to a temp file (a genuine Azure read) then parses it with
    ``dtype=str`` so bronze stays raw, no inferred types, original column names
    preserved. Fails loud with the blob name and row count on an empty read.
    """
    container = container or adls.get_container_client()
    with tempfile.TemporaryDirectory() as tmp:
        local = Path(tmp) / Path(blob_name).name
        size = adls.download_blob_to_path(blob_name, local, container)
        frame = pd.read_csv(local, dtype=str)
    if frame.empty:
        raise ValueError(
            f"Source blob {blob_name} ({size} bytes) parsed to 0 rows, refusing to "
            f"land an empty bronze batch."
        )
    print(f"[read_source] {blob_name}: {len(frame):,} rows, {len(frame.columns)} columns")
    return frame


def add_ingest_metadata(frame: pd.DataFrame, source_file: str) -> pd.DataFrame:
    """Append ``_source_file`` and ``_load_ts`` (UTC) to the raw frame.

    The load timestamp is stamped once here at read time so it is identical for
    every row in the batch and survives into the Delta table via Parquet.
    """
    stamped = frame.copy()
    stamped[SOURCE_FILE_COL] = source_file
    stamped[LOAD_TS_COL] = pd.Timestamp(datetime.now(UTC))
    return stamped


def _column_ddl(frame: pd.DataFrame) -> str:
    """Build the ``CREATE TABLE`` column list: every column STRING except _load_ts.

    The Unity Catalog account mask is bound **inline** on the identifier columns
    (``MASK`` clause), so masking is part of the table definition and applied
    atomically at creation. A separate ``ALTER ... SET MASK`` after load could be
    skipped if it failed while rows were already committed, leaving the identifiers
    permanently unmasked (the idempotency gate would short-circuit every re-run
    before re-binding). Inline binding removes that window.

    Fails loud if an identifier column that must be masked is absent from the frame,
    so bronze is never created with the account mask silently missing.
    """
    unmaskable = [column for column in MASKED_COLUMNS if column not in frame.columns]
    if unmaskable:
        raise ValueError(
            f"Refusing to create bronze without the account mask: columns {unmaskable} "
            f"are declared PII (MASKED_COLUMNS) but absent from the source "
            f"{list(frame.columns)}."
        )
    parts = []
    for column in frame.columns:
        sql_type = "TIMESTAMP" if column == LOAD_TS_COL else "STRING"
        mask = f" MASK {MASK_FUNCTION}" if column in MASKED_COLUMNS else ""
        parts.append(f"`{column}` {sql_type}{mask}")
    return ", ".join(parts)


def _already_loaded(source_file: str, client: WorkspaceClient, warehouse_id: str) -> bool:
    """Return True if a batch for ``source_file`` is already present in the table.

    The idempotency gate: keyed on ``_source_file`` so re-running the same source
    does not double-count. Returns False if the table does not yet exist.
    """
    # Qualify information_schema with the catalog: the warehouse's current catalog
    # is not paytrail, so an unqualified reference would look in the wrong place.
    exists = warehouse.scalar(
        "SELECT count(*) FROM paytrail.information_schema.tables "
        "WHERE table_schema='bronze' AND table_name='transactions_raw'",
        warehouse_id=warehouse_id,
        client=client,
    )
    if not exists or int(exists) == 0:
        return False
    count = warehouse.scalar(
        f"SELECT count(*) FROM {TABLE} WHERE {SOURCE_FILE_COL} = :source_file",
        warehouse_id=warehouse_id,
        client=client,
        parameters=[StatementParameterListItem(name="source_file", value=source_file)],
    )
    return count is not None and int(count) > 0


def _stage_parquet(frame: pd.DataFrame, source_file: str, client: WorkspaceClient) -> str:
    """Write the frame to Parquet and upload it into a per-source Volume subdir.

    ``COPY INTO`` reads a *directory*, so each source gets its own subdir keyed on
    a filesystem-safe form of its name; returns that directory path.
    """
    safe = source_file.replace("/", "__")
    subdir = f"{VOLUME_PATH}/{safe}"
    buffer = io.BytesIO()
    frame.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)
    client.files.upload(f"{subdir}/data.parquet", buffer, overwrite=True)
    return subdir


def write_bronze(
    frame: pd.DataFrame,
    source_file: str,
    warehouse_id: str | None = None,
    client: WorkspaceClient | None = None,
) -> int:
    """Append ``frame`` to the managed bronze table, idempotently.

    Skips entirely if a batch for ``source_file`` is already present. Otherwise
    creates the table if needed (with the account column mask bound inline in its
    definition, see :func:`_column_ddl`), stages Parquet into the Volume, and
    ``COPY INTO``s it. Returns the table's row count after load.
    """
    _validate_source_name(source_file)
    client = client or WorkspaceClient()
    warehouse_id = warehouse.resolve_warehouse_id(warehouse_id)

    if _already_loaded(source_file, client, warehouse_id):
        current = warehouse.scalar(
            f"SELECT count(*) FROM {TABLE}", warehouse_id=warehouse_id, client=client
        )
        print(f"[write_bronze] {source_file} already loaded, no-op (table has {current} rows)")
        return int(current or 0)

    warehouse.execute(f"CREATE VOLUME IF NOT EXISTS {VOLUME}", warehouse_id, client)
    warehouse.execute(
        f"CREATE TABLE IF NOT EXISTS {TABLE} ({_column_ddl(frame)}) "
        f"COMMENT 'Raw PaySim landing, append-only, string-typed + ingest metadata'",
        warehouse_id,
        client,
    )
    subdir = _stage_parquet(frame, source_file, client)
    warehouse.execute(
        f"COPY INTO {TABLE} FROM '{subdir}/' FILEFORMAT = PARQUET",
        warehouse_id,
        client,
    )
    total = warehouse.scalar(
        f"SELECT count(*) FROM {TABLE}", warehouse_id=warehouse_id, client=client
    )
    print(f"[write_bronze] loaded {source_file}; table now has {total} rows")
    return int(total or 0)


def ingest(blob_name: str, warehouse_id: str | None = None) -> int:
    """Full bronze ingest for one source blob: read → metadata → write. Returns rows.

    Checks the idempotency key *before* reading, so a no-op re-run of the full
    ~493 MB source does not download and parse it just to discard the result.
    """
    _validate_source_name(blob_name)
    client = WorkspaceClient()
    warehouse_id = warehouse.resolve_warehouse_id(warehouse_id)
    if _already_loaded(blob_name, client, warehouse_id):
        total = warehouse.scalar(
            f"SELECT count(*) FROM {TABLE}", warehouse_id=warehouse_id, client=client
        )
        print(f"[ingest] {blob_name} already loaded, skipping read (table has {total} rows)")
        return int(total or 0)
    frame = read_source(blob_name)
    stamped = add_ingest_metadata(frame, blob_name)
    return write_bronze(stamped, blob_name, warehouse_id=warehouse_id, client=client)


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-blob",
        default=DEFAULT_BLOB,
        help=f"ADLS blob to ingest (default: {DEFAULT_BLOB}; smoke: {SMOKE_BLOB})",
    )
    parser.add_argument(
        "--smoke",
        action="store_true",
        help=f"Shortcut for --source-blob {SMOKE_BLOB} (the 10k smoke path)",
    )
    parser.add_argument(
        "--warehouse-id",
        default=None,
        help="SQL warehouse id (default: $DATABRICKS_WAREHOUSE_ID)",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    """CLI entry point."""
    args = parse_args(argv)
    blob = SMOKE_BLOB if args.smoke else args.source_blob
    rows = ingest(blob, warehouse_id=args.warehouse_id)
    print(f"[bronze] done, {TABLE} holds {rows:,} rows after ingesting {blob}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
