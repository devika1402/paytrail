# Dashboards

The AI/BI (Databricks Lakeview) dashboard on `paytrail.gold.mart_daily_settled_volume`
is created and published code-first by [`setup/create_dashboard.py`](../../setup/create_dashboard.py)
(`make dashboard`, idempotent). Its serialised definition is version-controlled at
[`bi/paytrail_mart.lvdash.json`](../../bi/paytrail_mart.lvdash.json).

It has one page with:

- three **KPI counters**: total transactions, total settled volume, and the overall
  **fraud rate** (shown as a rate because fraud is concentrated in two types)
- a **total settled volume over time** trend (one clean line, summed across types)
- four **ranked, value-labelled horizontal bars** profiling each transaction type by a
  different metric, so magnitude reads despite the ~2,400x range across types:
  **settled volume**, **transaction count**, **average ticket size**, and **fraud rate**
- a **settled volume by customer segment** bar (the PM view)
- **transaction-type** and **customer-segment** filters driving the whole page.

The chart forms are deliberate: the raw data spans a huge range (TRANSFER ~485bn vs
DEBIT ~0.2bn), so overlapping lines on a shared linear axis would crush the small
series. Ranked bars with value labels keep every type readable and comparable.

## Screenshots

Free Edition workspaces are ephemeral and there is no public dashboard URL, so the
shareable artefact is a screenshot. The dashboard was captured on the full 6.36M-row
mart and saved here as two images (the page is taller than one viewport):

- [`daily_settled_volume_overview.png`](daily_settled_volume_overview.png), the top of
  the page: the transaction-type and customer-segment filters, the three KPI counters
  (6.36M total transactions, 1.14T total settled volume, 0.13% overall fraud rate), the
  settled-volume trend line, and the settled-volume-by-customer-segment bar.
- [`daily_settled_volume_by_type.png`](daily_settled_volume_by_type.png), the per-type
  profile: ranked horizontal bars of settled volume, transaction count, average ticket
  size, and fraud rate for each of the five transaction types.

Both images are embedded near the top of the root [README](../../README.md), beside the
"Reading it" caveats that explain the engineered fields. Those caveats should always
travel with the images, so a partly definitional bar or a synthetic date is not misread
as source data.
