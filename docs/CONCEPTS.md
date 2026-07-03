# Concepts reference

The paytrail pipeline relies on the below ideas. Pipeline docs point here instead of re-explaining.

---

## 1. Delta Lake internals

Delta Lake is the table format the whole pipeline is built on. It lets a set of data
files stored in cloud storage behave like a proper database table: versioned, and safe
for many writers at once. Concretely, Delta Lake is Parquet data files plus a transaction
log that turns a directory of files on object storage into a table with ACID guarantees
(a write either completes fully or not at all, and a reader never sees a half-written
state).

**Transaction log (`_delta_log/`).** Every write commits an ordered JSON file
(`00000000000000000000.json`, `...001.json`, …). Each commit is an atomic set of
*actions*: `add`/`remove` a data file, change `metadata`, set `protocol`. Table
state at any version is the replay of all commits up to that version. Every ~10
commits Delta writes a Parquet **checkpoint** that snapshots cumulative state so
readers don't replay the whole log, they load the latest checkpoint and only the
JSON commits after it. The log, read in order, defines which files belong to the table.

**ACID on object storage.** Object stores (S3/ADLS/GCS) provide per-object atomic
writes but no multi-file transactions. Delta gets atomicity from a single atomic
step: creating the next log file `N.json` as a put-if-absent. If two writers both
try to commit version `N`, only one wins the create, and the loser re-reads and retries.
This is **optimistic concurrency control**: a writer reads a snapshot, does its work,
and commits at `N+1`. On conflict it checks whether the other commit touched the same
files (read/write-set conflict detection) and either retries or fails. The result is
**snapshot isolation**, readers always see a consistent version, never a half-written
one.

**Time travel.** Because the log records every version and removed files aren't
deleted immediately, an earlier state can be read with `VERSION AS OF n` or
`TIMESTAMP AS OF t`. Uses: audit ("what did this table look like at settlement"),
reproducing a past run, rolling back a bad write.

**Small-file problem.** Frequent appends / streaming produce many tiny Parquet
files. Query cost then comes from per-file overhead (opening files, task scheduling,
reading footers) rather than useful bytes, and metadata bloats. This is the main
thing layout maintenance fixes.

**`OPTIMIZE` / `ZORDER` / `VACUUM`.**
- `OPTIMIZE` bin-packs many small files into fewer large (~1 GB) files. Faster scans,
  less metadata.
- `ZORDER BY (cols)` co-locates rows with nearby values in the z-ordered columns into
  the same files. Delta keeps per-file min/max stats, and better clustering means more
  files can be **skipped** (data skipping) for filtered queries. Best on high-cardinality
  columns used in filters and joins.
- `VACUUM` physically deletes data files no longer referenced by the log and older than
  a retention threshold (default 7 days). Reclaims storage but **shortens the
  time-travel window**, don't vacuum below the history still needed.

**Z-order vs. liquid clustering.** Z-order is a one-shot layout: the columns are re-specified
and re-run on `OPTIMIZE`, full re-clustering is expensive, and it handles
data skew poorly. **Liquid clustering** (`CLUSTER BY`) is the successor, clustering
keys are a table property, applied **incrementally** on write and on `OPTIMIZE`, keys
can change without rewriting all data, and it replaces the partitioning-vs-z-order
tradeoff. Preferred for new tables, which is why it is a candidate for the fact table's
filter columns.

---

## 2. The medallion pattern

The medallion pattern organises a warehouse into three stages, so raw data becomes
report-ready in clear, separable steps. Each layer answers one question about the data.

- **Bronze, "what arrived?"** The source is stored exactly as it came in, append-only,
  original column names intact, with `_source_file` and `_load_ts` recording the origin
  and load time of every row. There is no cleaning or retyping, because bronze exists to
  be a faithful, replayable record. If a downstream rule changes, the pipeline rebuilds from bronze
  rather than re-fetching the source.
- **Silver, "what is true?"** The data is typed, deduplicated on a documented grain, its
  keys made consistent, and each row checked against a contract. Rows that fail are
  quarantined with a reason instead of being discarded. Enforcing quality once, here,
  means every consumer downstream inherits the same guarantees, and keeping the
  failed rows means they can be investigated rather than lost.
- **Gold, "what does the business see?"** The data is shaped for its readers: a star
  schema and marts built for a specific consumer, like a regulatory-style report or a
  product manager's dashboard. Business meaning is applied once, at this layer, so
  consumers get a clean, read-optimised shape without the raw complexity.

Running through all three is separation of concerns: faithful input (bronze), dependable
data (silver), and business meaning (gold) are distinct jobs, and each layer rebuilds
from the one before it.

---

## 3. Unity Catalog (UC)

Unity Catalog is how the project controls who can see which data and keeps an automatic
record of where every table came from. It is the governance layer: a single metastore
(the central directory that records every catalog, schema, and table, and who may access
each) that governs data across workspaces.

**Three-level namespace:** `catalog → schema → object` (table / view / volume),
e.g. `paytrail.silver.transactions`. This replaces the older two-level
`hive_metastore.schema.table` and adds the catalog as a governance/isolation boundary.

**Managed vs. external tables.**
- *Managed*, UC owns both the metadata **and** the data files (in a UC-managed storage
  location). `DROP` deletes the data, and UC can auto-maintain the layout. This project uses
  managed tables throughout, because Free Edition storage is Databricks-managed.
- *External*, data is stored at a caller-specified cloud path, and UC manages metadata only.
  `DROP` leaves the files in place. Used when data must be stored in a specific bucket or is
  shared with non-Databricks tools.

**Lineage.** UC automatically captures table- and column-level lineage for operations
run through it, which query/job produced which table, and the upstream/downstream
graph, plus links to the jobs, notebooks, and dashboards involved. Governance and audit
without hand-maintained lineage. paytrail exports the bronze → silver → gold lineage from here.

UC also centralises permissions (`GRANT`), discovery, and search over these objects.

---

## 4. Databricks Asset Bundles (DABs)

Asset Bundles let the pipeline's deployment be defined in a file in the repo rather than
clicked together in a web console. This is infrastructure-as-code (the deployment setup
is defined in version-controlled files, so it is reviewable and repeatable), applied to
Databricks. A `databricks.yml` declares the bundle, its **resources** (jobs, pipelines,
…), and **targets** (e.g. `dev`, `prod`). The CLI:

- `databricks bundle validate`, check the config resolves.
- `databricks bundle deploy`, upload project files to the workspace and create/update
  the resources through the platform APIs.
- `databricks bundle run <resource>`, trigger a deployed job/pipeline.


---

## 5. Workflows / Lakeflow (mapped from Airflow DAGs)

Workflows is what runs the pipeline's steps in the right order, on a schedule or on
demand. **Databricks Workflows (Jobs)** is the native orchestrator. A **Job** is a DAG of
**tasks** with declared dependencies (`depends_on`). A DAG (directed acyclic graph) is
just a set of steps wired so each one waits for the steps it depends on, with no loops
back. Task types include dbt, Python
script/wheel, SQL, notebook, pipeline, and run-job. "**Lakeflow**" is the umbrella brand
for Databricks data engineering: Lakeflow Jobs (= Workflows), Lakeflow Declarative
Pipelines (= DLT), Lakeflow Connect (= managed ingestion).

For an Airflow user, the mapping is direct:

| Airflow | Databricks Workflows |
|---------|----------------------|
| DAG | Job |
| Task / Operator | Job task |
| `t1 >> t2` / `set_upstream` | task `depends_on` |
| Scheduler | Job schedule / trigger |
| XCom (small values) | task values |
| Executors + workers infra | Databricks compute (serverless or clusters), no separate infra |

The main difference: Workflows run **on** Databricks compute with native Delta/UC/lineage
integration, so there's no standalone scheduler to operate. (Where Airflow is already in use,
the Databricks provider can trigger these jobs instead of replacing it.) paytrail's job
DAG is `ingest_bronze → build_silver → build_gold → run_tests → benchmark`, with
`run_tests` as the governance gate.

---

## 6. Serverless + Predictive I/O

Serverless means the pipeline gets a query engine on demand, with nobody setting up or
sizing a cluster first. **Serverless SQL warehouses** are compute fully managed by
Databricks: near-instant start, automatic scaling, no cluster sizing. Compute is billed only for query
time. Free Edition runs
on serverless, and it's the warehouse every dbt and benchmark query targets.

**Predictive I/O** is a Photon-era engine feature that uses learned and heuristic
optimisations to speed reads (selective scans, efficient point lookups and updates via
deletion vectors) and writes, and to optimise layout and pruning automatically, without manual
tuning.

**Why this matters for the benchmark.** Because serverless applies
Predictive I/O and automatic optimisation by default, the "naive" arm of a
before/after benchmark is already partly optimised by the platform. A plain
naive-versus-`OPTIMIZE` comparison would therefore understate the delta, or measure the
platform's work rather than the layout change. So the benchmark isolates a difference it
explicitly creates (write the fact as many small files, then `OPTIMIZE` and
cluster), and states that limitation in the write-up.
