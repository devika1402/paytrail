{{
  config(
    materialized='table',
    tags=['gold'],
  )
}}

-- Account dimension: one conformed row per account, whether it appears as the
-- originator or the destination of a transaction. account_key is the tokenised
-- key from silver (no raw id here). account_type (customer/merchant) is a stable
-- property of the key, so aggregating it is safe. first/last seen bound the
-- account's activity window.

with appearances as (

    select
        orig_account_key as account_key,
        orig_account_type as account_type,
        event_ts
    from {{ ref('transactions') }}

    union all

    select
        dest_account_key as account_key,
        dest_account_type as account_type,
        event_ts
    from {{ ref('transactions') }}

)

select
    account_key,
    max(account_type) as account_type,
    min(event_ts) as first_seen_ts,
    max(event_ts) as last_seen_ts,
    count(*) as appearance_count
from appearances
group by account_key
