{{
  config(
    materialized='table',
    tags=['gold'],
  )
}}

-- Customer dimension: the customer-typed accounts, enriched with an originated
-- volume profile and a derived **segment** (a volume band) for the PM dashboard,
-- the segment is what a product manager slices markets by. Segment = tertile of
-- total originated amount (balanced high/medium/low), robust to absolute scale.

with customers as (

    select
        account_key,
        first_seen_ts,
        last_seen_ts
    from {{ ref('dim_account') }}
    where account_type = 'customer'

),

originated as (

    select
        orig_account_key as customer_key,
        sum(amount) as total_originated_amount,
        count(*) as originated_txn_count
    from {{ ref('transactions') }}
    where orig_account_type = 'customer'
    group by orig_account_key

)

select
    c.account_key as customer_key,
    c.first_seen_ts,
    c.last_seen_ts,
    coalesce(o.total_originated_amount, 0) as total_originated_amount,
    coalesce(o.originated_txn_count, 0) as originated_txn_count,
    case ntile(3) over (order by coalesce(o.total_originated_amount, 0))
        when 3 then 'high'
        when 2 then 'medium'
        else 'low'
    end as segment
from customers as c
left join originated as o
    on c.account_key = o.customer_key
