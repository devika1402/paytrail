# Governance, Unity Catalog column masking

This document is about keeping sensitive account identifiers out of the hands of anyone
with no business reason to see them, while still letting the pipeline itself process them.
The tool for that is a column mask: a rule attached to a column that returns the full
value to authorised users and a redacted version (for example `***7890`) to everyone else.

paytrail runs on **synthetic** PaySim data, so nothing here is genuine PII (personally
identifiable information, data that points to a specific person). The point of
this document is to demonstrate the *mechanism* a payments team operates on protected
production data, rather than just naming the catalog structure. A screener asking "how would
this change with protected production data?" should be able to read an operable answer here.

## What is applied

A Unity Catalog **column mask** on account identifiers. The masking function
`paytrail.bronze.mask_account` is created in `setup/uc_setup.sql`:

```sql
CREATE OR REPLACE FUNCTION paytrail.bronze.mask_account(account STRING)
  RETURNS STRING
  RETURN CASE
    WHEN is_member('paytrail_pii_readers') THEN account
    ELSE CONCAT('***', RIGHT(account, 4))
  END;
```

- Members of the `paytrail_pii_readers` group see the full account id.
- Everyone else sees `***` + the last 4 characters (e.g. `C1234567890` → `***7890`).

**Group scope** The function uses `is_member()`, which
checks a **workspace-local** group. Free Edition only lets us manage workspace
groups. Account-level groups (and `is_account_group_member`) need account-console
admin, which Free Edition does not expose. The mechanism is identical. Only the
group's scope differs. In production, this would be an account group federated from
the identity provider.

**Who is a member, the ETL/consumer split.** The pipeline (ETL) principal is a
member: it has a lawful basis to process raw identifiers, because silver must read
the raw `nameOrig`/`nameDest` to conform and **tokenise** account keys. Every
consumer principal outside the group, analysts, BI, sees `***`+last4. So raw
identifiers are visible only to the processing identity and are never propagated:
silver replaces them with a sha2 token, so nothing downstream of silver carries a
raw id at all (defence in depth: mask at the raw layer, tokenise at the trust
boundary). On Free Edition a single principal plays both roles, and a dedicated ETL
service principal separates them cleanly in production.

The sha2 token is a pseudonym rather than anonymisation. An account id is
low-entropy and structured, so an unsalted hash of it is recoverable by brute force
or a precomputed table, which means the token conceals the raw string from
downstream tables but is not a control-worthy anonymiser on its own. On production
data the token would use a keyed hash (HMAC, with the key held apart from the data)
or a separately-held salt, so the mapping cannot be recovered without the secret.

The mask is bound to a column with `ALTER TABLE ... ALTER COLUMN ... SET MASK`, and the
engine then applies the function to that column on every read, transparently, across all
query paths. There is no unmasked view to leak through. The two account-identifier
columns of the bronze table are bound like this:

```sql
ALTER TABLE paytrail.bronze.transactions_raw
  ALTER COLUMN nameOrig SET MASK paytrail.bronze.mask_account;
ALTER TABLE paytrail.bronze.transactions_raw
  ALTER COLUMN nameDest SET MASK paytrail.bronze.mask_account;
```

## Status on Databricks Free Edition

Applied and verified directly on the bronze table. The mask is bound to the
`nameOrig`/`nameDest` columns of `paytrail.bronze.transactions_raw` via `SET MASK`, and
both sides of the group gate have been observed:

- **Non-member read** (before the ETL principal was granted): `nameOrig` returned
  `***6815`, so the mask redacts for anyone outside `paytrail_pii_readers`.
- **Member read** (ETL principal granted): the same column returns the full
  `C1231006815`, so the pipeline can conform and tokenise keys.

`SET MASK` is available here and the group gate switches behaviour.

## How this would map to production payment-data handling

| Concern | Production requirement | What the mask demonstrates |
|---------|------------------------|----------------------------|
| **PII (GDPR)** | Personal identifiers exposed only to those with a lawful basis / need-to-know, with data minimisation. | Group-gated access. Only `paytrail_pii_readers` sees full identifiers, everyone else gets a minimised form. |
| **PCI-DSS** | Mask the PAN so at most the first 6 / last 4 are shown to those without a business need. | The `***` + last-4 pattern mirrors the standard PAN-truncation rule, with account ids standing in for the PAN here. |
| **Least privilege / audit** | Access governed centrally and logged. | The mask is enforced by Unity Catalog for every query path, and UC lineage/audit captures who read what, no per-query opt-in. |

On production data, the setup would additionally govern the group membership through the
identity provider, likely add a **row filter** (e.g. restrict by region/entity), and
route full-value access through a break-glass, audited role. The column mask is the
smallest workable proof of that operating model.
