{{
  config(
    materialized='table',
    tags=['gold'],
  )
}}

-- Static dimension: the five PaySim transaction types + human descriptions and a
-- coarse flow direction. Natural key = the type string (it is the join key on the
-- fact). Kept in-model (not a seed) because it is tiny, fixed, and self-documenting.

select
    transaction_type,
    description,
    flow_direction
from (
    values
    ('CASH_IN', 'Customer deposits cash into an account via a merchant.', 'inflow'),
    ('CASH_OUT', 'Customer withdraws cash from an account via a merchant.', 'outflow'),
    ('TRANSFER', 'Funds moved between customer accounts.', 'transfer'),
    ('PAYMENT', 'Customer pays a merchant for goods or services.', 'outflow'),
    ('DEBIT', 'Funds sent from an account to a bank.', 'outflow')
) as t (transaction_type, description, flow_direction)
