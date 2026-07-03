{{
  config(
    materialized='view',
    tags=['silver'],
  )
}}

-- Single typed + conformed + validated projection of bronze. Everything silver
-- needs is derived exactly once here:
--   * types, try_cast so a bad value becomes a quarantine reason, not a job crash
--   * keys, account ids tokenised (sha2) so no raw identifier flows past silver
--   * actor, C... = customer, M... = merchant (PaySim convention)
--   * event ts, derived from `step` (the event clock: 1 step = 1 simulated hour)
--   * contract, _quarantine_reason is NULL for a clean row, else the first breach
-- Downstream: transactions = clean rows deduped; transactions_quarantine = the rest.

with source as (

    select * from {{ source('bronze', 'transactions_raw') }}

),

typed as (

    select
        -- Raw identifiers kept only to validate + tokenise; never selected downstream.
        nameOrig,
        nameDest,
        type as txn_type,
        try_cast(step as int) as step_num,
        try_cast(amount as decimal(18, 2)) as amount,
        try_cast(oldbalanceOrg as decimal(18, 2)) as old_balance_orig,
        try_cast(newbalanceOrig as decimal(18, 2)) as new_balance_orig,
        try_cast(oldbalanceDest as decimal(18, 2)) as old_balance_dest,
        try_cast(newbalanceDest as decimal(18, 2)) as new_balance_dest,
        try_cast(isFraud as int) = 1 as is_fraud,
        try_cast(isFlaggedFraud as int) = 1 as is_flagged_fraud,
        _source_file,
        _load_ts
    from source

),

derived as (

    select
        -- Natural grain of one payment event, hashed. Dedup key + gold fact unique_key.
        sha2(concat_ws(
            '|',
            cast(step_num as string), txn_type, nameOrig, nameDest, cast(amount as string)
        ), 256) as transaction_id,
        -- Event clock: PaySim step 1 = the first simulated hour. Anchor date is a
        -- synthetic constant (the data carries no calendar dates), dim_date
        -- builds off this same expression.
        timestampadd(hour, step_num - 1, timestamp '2023-01-01 00:00:00') as event_ts,
        step_num,
        txn_type,
        amount,
        -- Tokenise identifiers: stable pseudonymous key, no raw id downstream.
        sha2(nameOrig, 256) as orig_account_key,
        case when left(nameOrig, 1) = 'M' then 'merchant' else 'customer' end
            as orig_account_type,
        old_balance_orig,
        new_balance_orig,
        sha2(nameDest, 256) as dest_account_key,
        case when left(nameDest, 1) = 'M' then 'merchant' else 'customer' end
            as dest_account_type,
        old_balance_dest,
        new_balance_dest,
        is_fraud,
        is_flagged_fraud,
        _source_file,
        _load_ts,
        -- Contract: first breach wins, else NULL (clean). Every check is a genuine
        -- corruption signal, so on clean PaySim data this is NULL for every row.
        case
            when nameOrig is null or nameOrig = '' or nameDest is null or nameDest = ''
                then 'null_account_key'
            when step_num is null or step_num < 1
                then 'invalid_step'
            when amount is null
                then 'unparseable_amount'
            when amount < 0
                then 'negative_amount'
            when
                coalesce(old_balance_orig, 0) < 0 or coalesce(new_balance_orig, 0) < 0
                or coalesce(old_balance_dest, 0) < 0 or coalesce(new_balance_dest, 0) < 0
                then 'negative_balance'
            when left(nameOrig, 1) not in ('C', 'M') or left(nameDest, 1) not in ('C', 'M')
                then 'unknown_actor_type'
        end as _quarantine_reason
    from typed

)

select * from derived
