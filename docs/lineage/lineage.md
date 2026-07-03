# Unity Catalog lineage, bronze → silver → gold

Captured automatically by Unity Catalog from executed queries, exported via the
lineage-tracking API (`setup/export_lineage.py`). Every gold figure traces back
to the raw bronze row it derives from.

## Edges (upstream → table)

- `paytrail.bronze.transactions_raw` _(source, lands from ADLS Gen2 via bronze ingest)_
- `paytrail.bronze.transactions_raw` → `paytrail.silver.stg_transactions`
- `paytrail.silver.stg_transactions` → `paytrail.silver.transactions`
- `paytrail.silver.stg_transactions` → `paytrail.silver.transactions_quarantine`
- `paytrail.silver.transactions` → `paytrail.gold.fact_transaction`
- `paytrail.silver.transactions` → `paytrail.gold.dim_account`
- `paytrail.gold.dim_account` → `paytrail.gold.dim_customer`
- `paytrail.silver.transactions` → `paytrail.gold.dim_customer`
- `paytrail.silver.transactions` → `paytrail.gold.dim_date`
- `paytrail.gold.dim_transaction_type` _(static, literal VALUES, no upstream)_
- `paytrail.gold.dim_customer` → `paytrail.gold.mart_daily_settled_volume`
- `paytrail.gold.dim_date` → `paytrail.gold.mart_daily_settled_volume`
- `paytrail.gold.fact_transaction` → `paytrail.gold.mart_daily_settled_volume`

## Flow

```
bronze: transactions_raw
silver: stg_transactions, transactions, transactions_quarantine
gold: fact_transaction, dim_account, dim_customer, dim_date, dim_transaction_type, mart_daily_settled_volume
```
