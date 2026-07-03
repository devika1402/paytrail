{{
  config(
    materialized='table',
    tags=['gold'],
  )
}}

-- Date dimension: a contiguous daily spine spanning the event window (derived from
-- silver's event_ts, which comes from PaySim `step`). A full spine, not just the
-- dates present, is the correct calendar dimension; because it is built from the
-- observed min/max it still covers every fact date, so the fact→dim_date
-- relationship holds.

with bounds as (

    select
        date(min(event_ts)) as start_date,
        date(max(event_ts)) as end_date
    from {{ ref('transactions') }}

),

spine as (

    select explode(sequence(start_date, end_date, interval 1 day)) as date_key
    from bounds

)

select
    date_key,
    year(date_key) as year,
    month(date_key) as month,
    day(date_key) as day_of_month,
    weekofyear(date_key) as week_of_year,
    date_format(date_key, 'MMMM') as month_name,
    date_format(date_key, 'EEEE') as weekday_name,
    dayofweek(date_key) as weekday_num,
    dayofweek(date_key) in (1, 7) as is_weekend
from spine
