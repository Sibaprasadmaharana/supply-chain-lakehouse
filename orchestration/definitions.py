"""
Dagster orchestration: simulated source activity -> raw ingestion -> dbt build.

Run locally:
    pip install -r requirements-orchestration.txt
    cd dbt_project && dbt deps && cd ..
    dagster dev -f orchestration/definitions.py
Then open http://localhost:3000
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    AssetSpec,
    Definitions,
    MaterializeResult,
    ScheduleDefinition,
    asset,
    define_asset_job,
    multi_asset,
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DUCKDB_PATH", str(ROOT / "warehouse" / "warehouse.duckdb"))
RAW_TABLES = ["carriers", "sellers", "products", "customers", "orders", "order_items", "shipments"]

dbt_project = DbtProject(project_dir=ROOT / "dbt_project", profiles_dir=ROOT / "dbt_project")
dbt_project.prepare_if_dev()


def _run(context: AssetExecutionContext, *args: str) -> None:
    result = subprocess.run([sys.executable, *args], cwd=ROOT, capture_output=True, text=True)
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(result.stderr)


@asset(group_name="source_simulation", description="Advance the simulated OMS by one business day.")
def oms_activity(context: AssetExecutionContext) -> None:
    _run(context, "generator/generate.py", "batch", "--days", "1")


@multi_asset(
    specs=[
        AssetSpec(AssetKey(["raw", t]), deps=[AssetKey("oms_activity")], group_name="ingestion")
        for t in RAW_TABLES
    ]
)
def raw_tables(context: AssetExecutionContext):
    """Incremental extract-load from Postgres into the DuckDB raw layer.
    Asset keys match the dbt source keys, so Dagster wires lineage into dbt automatically."""
    _run(context, "ingestion/extract_load.py")
    for t in RAW_TABLES:
        yield MaterializeResult(asset_key=AssetKey(["raw", t]))


@dbt_assets(manifest=dbt_project.manifest_path)
def warehouse_models(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


daily_pipeline = define_asset_job("daily_pipeline", selection=AssetSelection.all())
daily_schedule = ScheduleDefinition(
    job=daily_pipeline, cron_schedule="0 6 * * *", execution_timezone="Asia/Kolkata"
)

defs = Definitions(
    assets=[oms_activity, raw_tables, warehouse_models],
    jobs=[daily_pipeline],
    schedules=[daily_schedule],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)
