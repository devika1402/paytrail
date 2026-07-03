# Azure integration: ADLS Gen2 source storage

The project's one use of Azure is storage: the source data file is kept in an Azure cloud
storage account and read from there when the pipeline starts. That storage service is
ADLS Gen2 (Azure Data Lake Storage Gen2, Azure's cloud file store built for large
analytics datasets).

The PaySim source file is stored in and read from Azure Data Lake Storage
Gen2. This is the project's Azure surface, scoped exactly to
storage integration on Azure (ADLS Gen2): storing the source blob and reading it
back in Python. It is not an Azure Databricks deployment: compute runs on
Databricks Free Edition, which is AWS-hosted (see the restriction below).

## What was done

| Item | Value |
|------|-------|
| Storage account | `paytrailstore` (ADLS Gen2, hierarchical namespace enabled) |
| Container | `paysim` |
| Full source blob | `raw/paysim.csv`, 493,534,783 bytes, ~6.3M rows (PaySim `ealaxi/paysim1`) |
| Smoke-sample blob | `raw/paysim_sample_10k.csv`, 739,436 bytes, header + 10,000 rows |
| Upload (code-first) | [`setup/land_source.py`](../setup/land_source.py), Kaggle download → 10k sample → upload both, idempotent (skips existing blobs) |
| Azure access module | [`src/paytrail/azure/adls.py`](../src/paytrail/azure/adls.py), the single place that talks to ADLS (upload / exists / streaming read) |
| Verified round trip | `download_blob_to_path` streamed the sample back from ADLS and parsed 10,000 rows + the PaySim header |

The 10k sample exists so every later task can iterate on a cheap slice, the
Free Edition quota killer is repeated full-6.3M runs, so the full CSV is read
end-to-end only once, last.

## Auth method

Access uses an account-key connection string kept in a git-ignored `.env`. No key is
committed, and none is printed by the tooling.

## Tier chosen, and why not the UC external-location tier

The most tightly governed way to connect Azure storage to the warehouse is
not available on the free tier, so the pipeline reads the file directly from Azure in
Python instead. The detail below shows how that was verified.

There are three possible tiers of integration. The strongest one Free Edition allows was taken.

- **Tier 2 (preferred), UC external location + storage credential: NOT AVAILABLE
  on Free Edition.** Verified by direct test:
  - The Free Edition metastore is **AWS-hosted** (`databricks metastores summary`
    → `"cloud": "aws"`, `region: us-east-2`, S3-backed) with
    `external_access_enabled: false`. Only the system-managed
    `__databricks_managed_storage_credential` / `__databricks_managed_storage_location`
    exist.
  - Creating an external location is privilege-gated:
    `databricks external-locations create …` →
    `User does not have CREATE EXTERNAL LOCATION on Credential '__databricks_managed_storage_credential'`.
  - A UC storage credential for Azure requires a dedicated **Azure service principal**
    (Directory ID / Application ID / Client Secret). UC does **not** accept the
    account key we have. `databricks storage-credentials create …` →
    `Service principal configuration is not valid`.
  - Net: cross-cloud (AWS-hosted UC → Azure ADLS) governed access would need an
    Azure AD app registration + a storage credential the Free Edition user isn't
    entitled to create. Out of scope, and no paid resource was provisioned.

- **Tier 3 (reliable fallback), chosen.** `src/paytrail/azure/adls.py` reads the
  source directly from ADLS Gen2 with the Azure SDK (`azure-storage-blob`) using
  account-key auth. The Azure read happens in Python, so bronze ingest
  performs a genuine Azure integration regardless of the UC restriction above.