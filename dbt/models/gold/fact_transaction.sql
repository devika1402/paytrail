{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='transaction_id',
    tags=['gold'],
  )
}}

-- Fact: one row per settled transaction (grain = transaction_id, the tokenised
-- hash of the natural grain from silver). Foreign keys reference every dimension;
-- measures are amount + the fraud flags.
--
-- Incremental via Delta MERGE on transaction_id: only rows loaded since the last
-- run are processed, and the merge match grain (transaction_id) makes a re-run
-- idempotent, a replayed silver row updates in place instead of duplicating.

select
    transaction_id,
    date(event_ts) as date_key,
    txn_type as transaction_type,
    orig_account_key,
    dest_account_key,
    -- Customer FK only when the originator is a customer (nulls are ignored by the
    -- relationships test); merchants have no customer-segment row.
    case when orig_account_type = 'customer' then orig_account_key end
        as orig_customer_key,
    amount,
    is_fraud,
    is_flagged_fraud,
    event_ts,
    _load_ts
from {{ ref('transactions') }}

{% if is_incremental() %}
    where _load_ts > (select coalesce(max(_load_ts), timestamp '1900-01-01') from {{ this }})
{% endif %}
