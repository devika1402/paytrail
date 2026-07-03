"""ADLS Gen2 access for paytrail, the project's Azure surface.

The PaySim source is stored in and read from **Azure Data Lake Storage Gen2**;
the Azure integration is genuine, not simulated. Auth is an account-key
**connection string** kept in a
git-ignored ``.env``; that string contains ``;`` separators and therefore cannot
be sourced in a POSIX shell, so it is always read here in Python.

This module is the single place that talks to Azure Blob/ADLS. The landing
script (``setup/land_source.py``) uses it to upload; bronze ingest uses
``download_blob_to_path`` to stream the source to a temp file without keeping
a full local copy.

Scope is honest: *storage integration on Azure (ADLS Gen2)*, object upload,
existence checks, and streaming reads via account-key auth. It is not an Azure
Databricks deployment (compute runs on Databricks Free Edition).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient, ContainerClient
from dotenv import load_dotenv

_CONN_STR_ENV = "AZURE_STORAGE_CONNECTION_STRING"
_CONTAINER_ENV = "ADLS_CONTAINER"


@dataclass(frozen=True)
class AdlsConfig:
    """Resolved ADLS connection settings, read from the environment / ``.env``."""

    connection_string: str
    container: str


def load_config(env_file: Path | None = None) -> AdlsConfig:
    """Read the ADLS connection string and container name from ``.env`` / env.

    Args:
        env_file: optional explicit path to a ``.env`` file. When ``None``,
            python-dotenv searches upward from the CWD. Existing environment
            variables always win over the file (so CI secrets override ``.env``).

    Raises:
        RuntimeError: if either required variable is missing, with the exact
            variable names so the fix is obvious.
    """
    load_dotenv(dotenv_path=env_file, override=False)
    connection_string = os.environ.get(_CONN_STR_ENV, "").strip()
    container = os.environ.get(_CONTAINER_ENV, "").strip()
    missing = [
        name
        for name, value in ((_CONN_STR_ENV, connection_string), (_CONTAINER_ENV, container))
        if not value
    ]
    if missing:
        raise RuntimeError(
            f"Missing ADLS config: {', '.join(missing)}. "
            f"Set them in .env (see .env comments), the connection string comes "
            f"from the storage account's Access keys blade."
        )
    return AdlsConfig(connection_string=connection_string, container=container)


def get_container_client(config: AdlsConfig | None = None) -> ContainerClient:
    """Return a client for the source container, creating the container if absent.

    Container creation is idempotent: an already-existing container is treated as
    success, so repeated runs (and running before/after manual portal setup) both
    work.
    """
    config = config or load_config()
    service = BlobServiceClient.from_connection_string(config.connection_string)
    container = service.get_container_client(config.container)
    try:
        container.create_container()
    except ResourceExistsError:
        pass
    return container


def blob_exists(blob_name: str, container: ContainerClient | None = None) -> bool:
    """Return True if ``blob_name`` already exists in the container."""
    container = container or get_container_client()
    return container.get_blob_client(blob_name).exists()


def upload_file(
    local_path: Path,
    blob_name: str,
    container: ContainerClient | None = None,
    overwrite: bool = False,
) -> int:
    """Upload ``local_path`` to ``blob_name``; return the byte size uploaded.

    Args:
        overwrite: when False (default), an existing blob is a hard error, the
            landing script decides skip-vs-replace, so this stays explicit and
            never silently clobbers a landed source file.

    Raises:
        FileNotFoundError: if ``local_path`` does not exist.
        RuntimeError: if the blob exists and ``overwrite`` is False.
    """
    if not local_path.is_file():
        raise FileNotFoundError(f"Cannot upload, local file not found: {local_path}")
    container = container or get_container_client()
    blob = container.get_blob_client(blob_name)
    if not overwrite and blob.exists():
        raise RuntimeError(
            f"Blob already exists: {blob_name}. Pass overwrite=True to replace it."
        )
    size = local_path.stat().st_size
    with local_path.open("rb") as handle:
        blob.upload_blob(handle, overwrite=overwrite, length=size)
    return size


def download_blob_to_path(
    blob_name: str,
    dest_path: Path,
    container: ContainerClient | None = None,
) -> int:
    """Stream a blob from ADLS to a local file; return the byte size downloaded.

    Streams in bounded chunks so a multi-hundred-MB source never has to fit in
    memory. This is the genuine Azure *read* used by bronze ingest: the
    source of truth is the ADLS container, and this pulls it down for loading.

    Raises:
        RuntimeError: if the blob does not exist, naming it for a fast diagnosis.
    """
    container = container or get_container_client()
    blob = container.get_blob_client(blob_name)
    if not blob.exists():
        raise RuntimeError(
            f"Source blob not found in ADLS: {blob_name}. Run `make setup` to land it."
        )
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    downloader = blob.download_blob(max_concurrency=2)
    written = 0
    with dest_path.open("wb") as handle:
        for chunk in downloader.chunks():
            handle.write(chunk)
            written += len(chunk)
    return written
