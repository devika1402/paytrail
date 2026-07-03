-- Governance gate: fail the build if the silver boundary does not reconcile.
-- Returns the offending audit row when either the count identity or the amount
-- identity breaks (bronze ≠ quarantine + clean_eligible). Green = nothing lost.
select *
from {{ ref('audit_silver_reconciliation') }}
where
    not rows_tie
    or not amount_tie
