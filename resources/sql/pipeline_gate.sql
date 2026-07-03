-- Pipeline governance gate, the task the deployed Databricks Workflow runs on
-- serverless. It fails the job run (raising USER_RAISED_EXCEPTION) if the pipeline
-- did not produce a trustworthy result: gold layer empty, or the silver
-- reconciliation identity broken. This is a post-transform data-quality gate, the
-- kind of check a scheduled Workflow enforces in production.
--
-- Why a SQL task and not a dbt task: the dbt-task type needs classic job compute,
-- which Free Edition does not provide (serverless only). dbt therefore owns the
-- transform DAG from the Makefile (the
-- planned fallback); this serverless gate is what the deployed job executes.
SELECT
    assert_true(
        (SELECT count(*) FROM paytrail.gold.fact_transaction) > 0,
        'gate: gold.fact_transaction is empty'
    ) AS fact_ok,
    assert_true(
        (SELECT count(*) FROM paytrail.gold.mart_daily_settled_volume) > 0,
        'gate: gold.mart_daily_settled_volume is empty'
    ) AS mart_ok,
    assert_true(
        (SELECT bool_and(rows_tie AND amount_tie)
         FROM paytrail.silver.audit_silver_reconciliation),
        'gate: silver reconciliation identity is broken'
    ) AS reconciliation_ok;
