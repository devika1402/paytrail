{{
  config(
    materialized='table',
    tags=['silver'],
  )
}}

-- Silver quarantine: rows that failed a data contract. They are captured, not
-- dropped, a payments pipeline must be able to prove nothing was silently lost,
-- and the quarantine count is a data-quality signal in its own right. Each row
-- carries the specific _quarantine_reason it breached. Not deduplicated: every
-- bad row is retained for audit.

select
    transaction_id,
    event_ts,
    step_num,
    txn_type,
    amount,
    orig_account_key,
    orig_account_type,
    old_balance_orig,
    new_balance_orig,
    dest_account_key,
    dest_account_type,
    old_balance_dest,
    new_balance_dest,
    is_fraud,
    is_flagged_fraud,
    _source_file,
    _load_ts,
    _quarantine_reason
from {{ ref('stg_transactions') }}
where _quarantine_reason is not null
