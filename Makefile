# paytrail, payments medallion lakehouse
# One command to rule them all: `make all`.
# Auth is OAuth U2M via the Databricks CLI profile below.

DATABRICKS_PROFILE ?= paytrail
export DATABRICKS_CONFIG_PROFILE = $(DATABRICKS_PROFILE)
# Serverless SQL warehouse id, used by setup SQL and dbt. Set it in your env or .env.
export DATABRICKS_WAREHOUSE_ID ?= your-warehouse-id

VENV   ?= .venv
DBT    ?= $(VENV)/bin/dbt
PYTHON ?= $(VENV)/bin/python

.DEFAULT_GOAL := help
.PHONY: help preflight uc setup deploy ingest ingest-full dbt benchmark lineage dashboard all clean

help: ## List available targets
	@echo "paytrail targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  %-10s %s\n", $$1, $$2}'

preflight: ## Capability pre-flight check
	@echo "[preflight] verifying the Databricks toolchain"

uc: ## Governance setup (idempotent): catalog + schemas + mask function + pii group
	$(PYTHON) setup/run_sql.py setup/uc_setup.sql
	$(PYTHON) setup/pii_group.py

setup: ## Download PaySim from Kaggle + land full CSV and 10k sample in ADLS Gen2
	$(PYTHON) setup/land_source.py

deploy: ## Validate and deploy the asset bundle
	databricks bundle validate
	databricks bundle deploy

ingest: ## Bronze ingest of the 10k smoke sample (safe default, protects the quota)
	$(PYTHON) -m paytrail.ingest.bronze --smoke

ingest-full: ## Bronze ingest of the full ~6.3M-row file (run once, last, quota guard)
	$(PYTHON) -m paytrail.ingest.bronze --source-blob raw/paysim.csv

dbt: ## dbt build, silver and gold models, data tests, unit tests, reconciliation gate
	$(DBT) build --profiles-dir dbt --project-dir dbt

benchmark: ## Layout benchmark: fragment fact -> rollup -> OPTIMIZE+ZORDER -> rollup; write docs/benchmark
	$(PYTHON) -m paytrail.benchmark.run_benchmark

lineage: ## Export Unity Catalog lineage (bronze->silver->gold) to docs/lineage/
	$(PYTHON) setup/export_lineage.py

dashboard: ## Create/publish the AI/BI (Lakeview) dashboard on the gold mart (idempotent)
	$(PYTHON) setup/create_dashboard.py

all: ## One command: governance setup -> deploy -> ingest -> dbt build+tests -> run gate job -> benchmark
	$(MAKE) uc
	$(MAKE) deploy
	$(MAKE) ingest
	$(MAKE) dbt
	databricks bundle run paytrail_pipeline
	$(MAKE) benchmark
	@echo "[all] pipeline complete, deployed Workflow gate passed on serverless"

clean: ## Tear down deployed Databricks resources
	databricks bundle destroy --auto-approve
