{{
  config(
    materialized='view',
    tags=['silver'],
  )
}}

-- Reconciliation control: proves every bronze row is accounted for across the
-- silver boundary, by count AND by amount. A payments pipeline that cannot show
-- nothing was silently lost is not trustworthy, so this is a first-class model
-- (surfaced) backed by a build-failing test (assert_silver_reconciliation).
--
-- Identity:  bronze = quarantine + clean_eligible
--            clean_eligible = clean + duplicates_removed   (dedup only removes)
--   ⇒ every bronze row is quarantined, kept, or a collapsed duplicate, nothing
--     vanishes. Amounts tie the same way (unparseable amounts sum to NULL/0 on
--     both sides, so they cancel).

with bronze as (
    select
        count(*) as rows,
        sum(try_cast(amount as decimal(18, 2))) as amount
    from {{ source('bronze', 'transactions_raw') }}
),

clean_eligible as (
    select
        count(*) as rows,
        sum(amount) as amount
    from {{ ref('stg_transactions') }}
    where _quarantine_reason is null
),

clean as (
    select
        count(*) as rows,
        sum(amount) as amount
    from {{ ref('transactions') }}
),

quarantine as (
    select
        count(*) as rows,
        sum(amount) as amount
    from {{ ref('transactions_quarantine') }}
)

select
    bronze.rows as bronze_rows,
    quarantine.rows as quarantine_rows,
    clean_eligible.rows as clean_eligible_rows,
    clean.rows as clean_rows,
    clean_eligible.rows - clean.rows as duplicates_removed,
    -- Count identity: bronze = quarantine + clean_eligible.
    (bronze.rows = quarantine.rows + clean_eligible.rows) as rows_tie,
    bronze.amount as bronze_amount,
    quarantine.amount as quarantine_amount,
    clean_eligible.amount as clean_eligible_amount,
    clean.amount as clean_amount,
    -- Amount identity: bronze total = quarantine + clean_eligible (NULLs cancel).
    (
        coalesce(bronze.amount, 0)
        = coalesce(quarantine.amount, 0) + coalesce(clean_eligible.amount, 0))
        as amount_tie
from bronze, quarantine, clean_eligible, clean
