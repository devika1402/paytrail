{{
  config(
    materialized='table',
    tags=['silver'],
  )
}}

-- Silver clean: contract-passing transactions, deduplicated to one row per event.
--
-- Grain: one settled payment event, identified by transaction_id (a hash of
--   step + type + orig + dest + amount).
-- Dedup + idempotency: a replayed bronze batch re-presents identical grains; we
--   keep exactly one row per transaction_id, so re-running never inflates silver.
-- Out-of-order policy: `step` (→ event_ts) is the EVENT clock; `_load_ts` is the
--   ARRIVAL clock. Ordering/consumption is by event_ts, never by arrival, a row
--   that lands late (higher _load_ts, lower step) still sorts to its true event
--   position. When the SAME grain arrives more than once, the most recently
--   loaded version wins (latest _load_ts), i.e. a correction supersedes the
--   original. That is the deterministic tiebreaker below.

with clean as (

    select * from {{ ref('stg_transactions') }}
    where _quarantine_reason is null

),

deduped as (

    select
        *,
        row_number() over (
            partition by transaction_id
            order by _load_ts desc, event_ts asc
        ) as _row_rank
    from clean

)

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
    _load_ts
from deduped
where _row_rank = 1
