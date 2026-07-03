{{
  config(
    materialized='table',
    tags=['gold'],
  )
}}

-- The one aggregated mart, serving two consumers from the same governed, reconciled
-- grain (day × transaction type × customer segment):
--
--   * A regulatory-reporting-style aggregate, `settled_amount` / `transaction_count`
--     per day is the auditable, reconciled daily figure a bank would file with a
--     financial regulator. (Synthetic data; this demonstrates the reporting PATTERN
--     and files nothing. No real regulator is named anywhere.)
--   * A product-manager dashboard, the `customer_segment` and `transaction_type`
--     splits are how a PM explores markets and segment behaviour.
--
-- Built from the star: fact joined to dim_date, dim_transaction_type (implicit via
-- the type key), and dim_customer for the segment.

select
    md5(concat_ws('|', cast(d.date_key as string), f.transaction_type, dc.segment))
        as mart_key,
    d.date_key,
    f.transaction_type,
    dc.segment as customer_segment,
    sum(f.amount) as settled_amount,
    count(*) as transaction_count,
    sum(case when f.is_fraud then 1 else 0 end) as fraud_count
from {{ ref('fact_transaction') }} as f
inner join {{ ref('dim_date') }} as d
    on f.date_key = d.date_key
inner join {{ ref('dim_customer') }} as dc
    on f.orig_customer_key = dc.customer_key
group by d.date_key, f.transaction_type, dc.segment
