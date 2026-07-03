-- Unity Catalog structure for paytrail. Idempotent: safe to re-run.
-- Executed via setup/run_sql.py against the serverless SQL warehouse.
-- Managed tables throughout (Free Edition storage is Databricks-managed).

-- Catalog: the top governance boundary.
CREATE CATALOG IF NOT EXISTS paytrail
  COMMENT 'paytrail payments medallion lakehouse';

-- Medallion schemas.
CREATE SCHEMA IF NOT EXISTS paytrail.bronze
  COMMENT 'Raw, append-only landing of source data + ingest metadata';

CREATE SCHEMA IF NOT EXISTS paytrail.silver
  COMMENT 'Typed, deduplicated, conformed, contract-validated - bad rows quarantined';

CREATE SCHEMA IF NOT EXISTS paytrail.gold
  COMMENT 'Star schema + reporting marts for consumers';

-- Governance mechanic: a Unity Catalog column-masking function.
-- Partially redacts account identifiers to '***' + last 4 chars for anyone
-- outside the paytrail_pii_readers group. This is the mechanism a bank operates
-- on PII/PCI-DSS-scoped columns; see docs/GOVERNANCE.md. Applied to the real
-- nameOrig/nameDest columns once bronze.transactions_raw exists.
--
-- Group scope: is_member() checks a WORKSPACE-local group. Free Edition only
-- lets us manage workspace groups (account groups + is_account_group_member need
-- account-console admin, which Free Edition does not expose). The mechanism is
-- identical; only the group's scope differs. The ETL principal is a member (it
-- has lawful basis to process raw identifiers for key conformance in silver);
-- every consumer principal outside the group sees ***last4.
CREATE OR REPLACE FUNCTION paytrail.bronze.mask_account(account STRING)
  RETURNS STRING
  COMMENT 'Column mask: full value for paytrail_pii_readers members, else ***last4'
  RETURN CASE
    WHEN is_member('paytrail_pii_readers') THEN account
    ELSE CONCAT('***', RIGHT(account, 4))
  END;
