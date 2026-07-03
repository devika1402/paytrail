"""Land the PaySim source into ADLS Gen2.

Code-first source landing, no portal clicks. This:

1. Downloads the PaySim dataset from Kaggle into ``data/`` if it is not already
   there (~493 MB, one file, git-ignored).
2. Derives a **10k-row smoke sample** so every later task can iterate on a cheap
   slice (the Free Edition quota killer is repeated full-6.3M runs).
3. Uploads both the full CSV and the sample into the ADLS Gen2 container, which
   is bronze's source of truth (read from there, not from the local disk).

Uploads are idempotent: an already-landed blob is skipped unless ``--overwrite``
is passed, so re-running is a no-op rather than a re-upload.

Usage:
    python setup/land_source.py                # download (if needed) + upload
    python setup/land_source.py --skip-full    # only the 10k sample (fast smoke)
    python setup/land_source.py --overwrite     # replace blobs that already exist
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from azure.storage.blob import ContainerClient

from paytrail.azure import adls

KAGGLE_DATASET = "ealaxi/paysim1"
SOURCE_FILENAME = "PS_20174392719_1491204439457_log.csv"

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
LOCAL_FULL = DATA_DIR / SOURCE_FILENAME
LOCAL_SAMPLE = DATA_DIR / "paysim_sample_10k.csv"

FULL_BLOB = "raw/paysim.csv"
SAMPLE_BLOB = "raw/paysim_sample_10k.csv"
SAMPLE_ROWS = 10_000


def download_full_csv() -> None:
    """Download + unzip the PaySim CSV from Kaggle into ``data/`` if absent.

    Shells out to the Kaggle CLI (auth is ``~/.kaggle/kaggle.json``). Fails loud
    with the CLI's own stderr so a missing token or unaccepted-terms is obvious.
    """
    if LOCAL_FULL.is_file():
        print(f"[download] present, skipping: {LOCAL_FULL.name} "
              f"({LOCAL_FULL.stat().st_size:,} bytes)")
        return
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[download] fetching {KAGGLE_DATASET} -> {DATA_DIR} (~493 MB)")
    result = subprocess.run(
        ["kaggle", "datasets", "download", "-d", KAGGLE_DATASET,
         "-p", str(DATA_DIR), "--unzip"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Kaggle download failed (exit {result.returncode}). "
            f"Check ~/.kaggle/kaggle.json and that the PaySim terms are accepted.\n"
            f"stderr: {result.stderr.strip()}"
        )
    if not LOCAL_FULL.is_file():
        raise RuntimeError(
            f"Kaggle download reported success but {LOCAL_FULL} is missing. "
            f"Expected file {SOURCE_FILENAME} in the dataset."
        )
    print(f"[download] done: {LOCAL_FULL.stat().st_size:,} bytes")


def build_sample(rows: int = SAMPLE_ROWS) -> None:
    """Write a header + ~``rows`` **systematic** smoke sample from the full CSV.

    Takes every N-th data row (stride = total_rows // rows) so the sample spans the
    entire event window, all ~744 PaySim steps / 30 days, instead of just the first
    few hours a head() slice would capture. That keeps every downstream layer
    (silver dedup/out-of-order, gold dim_date, the mart, the benchmark) time-varied
    on the cheap smoke path. Deterministic (fixed stride, no RNG) so re-runs are
    reproducible. Streams line-by-line; the 6.3M-row source is never held in memory.
    """
    if LOCAL_SAMPLE.is_file():
        print(f"[sample] present, skipping: {LOCAL_SAMPLE.name}")
        return
    if not LOCAL_FULL.is_file():
        raise FileNotFoundError(
            f"Cannot build sample, full CSV missing: {LOCAL_FULL}. "
            f"Run without --skip-full first."
        )
    total_rows = _count_data_rows(LOCAL_FULL)
    stride = max(1, total_rows // rows)
    print(f"[sample] {total_rows:,} source rows; stride={stride}; "
          f"writing systematic sample -> {LOCAL_SAMPLE.name}")
    written = 0
    with LOCAL_FULL.open("r", encoding="utf-8") as src, \
            LOCAL_SAMPLE.open("w", encoding="utf-8") as dst:
        header = src.readline()
        if not header:
            raise ValueError(f"Source CSV is empty: {LOCAL_FULL}")
        dst.write(header)
        for index, line in enumerate(src):
            if index % stride == 0:
                dst.write(line)
                written += 1
                if written >= rows:
                    break
    print(f"[sample] done: {written:,} data rows spanning the full event window")


def _count_data_rows(path: Path) -> int:
    """Count data rows (excluding the header) by streaming the file once."""
    with path.open("r", encoding="utf-8") as handle:
        total = sum(1 for _ in handle)
    return max(0, total - 1)


def upload(
    local_path: Path, blob_name: str, container: ContainerClient, overwrite: bool
) -> None:
    """Upload one file to ADLS, skipping if it already exists (unless overwrite)."""
    if not overwrite and adls.blob_exists(blob_name, container):
        print(f"[upload] already in ADLS, skipping: {blob_name}")
        return
    size = adls.upload_file(local_path, blob_name, container, overwrite=overwrite)
    print(f"[upload] landed {blob_name} ({size:,} bytes)")


def verify(blob_name: str, container: ContainerClient) -> None:
    """Confirm a blob is readable from ADLS (proves the round trip)."""
    if not adls.blob_exists(blob_name, container):
        raise RuntimeError(f"Post-upload verify failed, blob not found: {blob_name}")
    props = container.get_blob_client(blob_name).get_blob_properties()
    print(f"[verify] ADLS read OK: {blob_name} ({props.size:,} bytes)")


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-full", action="store_true",
                        help="Only land the 10k sample (skip the ~493 MB full CSV)")
    parser.add_argument("--overwrite", action="store_true",
                        help="Replace blobs that already exist in ADLS")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    """CLI entry point: download, sample, upload, verify."""
    args = parse_args(argv)
    container = adls.get_container_client()

    if not args.skip_full:
        download_full_csv()
        upload(LOCAL_FULL, FULL_BLOB, container, args.overwrite)
        verify(FULL_BLOB, container)

    build_sample()
    upload(LOCAL_SAMPLE, SAMPLE_BLOB, container, args.overwrite)
    verify(SAMPLE_BLOB, container)

    print("[land_source] PaySim source landed in ADLS Gen2.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
