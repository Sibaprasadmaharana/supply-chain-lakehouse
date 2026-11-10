#!/usr/bin/env python
"""
Incremental Extract-Load: Postgres (OMS) -> DuckDB raw layer (bronze).

Design:
  * DuckDB's postgres extension ATTACHes the source read-only, so extraction is plain SQL.
  * High-watermark per table = max(updated_at) already in raw.
  * A LOOKBACK window (default 48h) re-reads recent rows to catch late-arriving
    records. That creates duplicates in raw ON PURPOSE; staging dedupes them.
  * Raw is append-only (full change history) + lineage columns _batch_id, _loaded_at.
  * Every load is written to raw._load_audit for observability.
  * Re-running is safe (idempotent downstream): duplicates are removed in staging.

Usage:
    python ingestion/extract_load.py              # incremental
    python ingestion/extract_load.py --full-refresh
"""
from __future__ import annotations

import argparse
import os
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DUCKDB_PATH = os.getenv("DUCKDB_PATH", str(ROOT / "warehouse" / "warehouse.duckdb"))
LOOKBACK_HOURS = int(os.getenv("INGEST_LOOKBACK_HOURS", "48"))
TABLES = ["carriers", "sellers", "products", "customers", "orders", "order_items", "shipments"]

PG_DSN = (
    f"host={os.getenv('PG_HOST', 'localhost')} port={os.getenv('PG_PORT', '5432')} "
    f"dbname={os.getenv('PG_DB', 'supply')} user={os.getenv('PG_USER', 'de_user')} "
    f"password={os.getenv('PG_PASSWORD', 'de_pass')}"
)


def table_exists(con, table: str) -> bool:
    return con.execute(
        "select count(*) from information_schema.tables where table_schema = 'raw' and table_name = ?",
        [table],
    ).fetchone()[0] > 0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--full-refresh", action="store_true", help="drop raw tables and reload everything")
    args = parser.parse_args()

    Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH)
    con.execute("install postgres")
    con.execute("load postgres")
    con.execute(f"attach '{PG_DSN}' as src (type postgres, read_only)")
    con.execute("create schema if not exists raw")
    con.execute(
        """
        create table if not exists raw._load_audit (
            batch_id       varchar,
            table_name     varchar,
            watermark_from timestamp,
            rows_loaded    bigint,
            max_updated_at timestamp,
            started_at     timestamp,
            finished_at    timestamp
        )
        """
    )

    batch_id = uuid.uuid4().hex[:12]
    print(f"[ingest] batch {batch_id} -> {DUCKDB_PATH}")
    for table in TABLES:
        started = datetime.now()
        if args.full_refresh:
            con.execute(f"drop table if exists raw.{table}")

        if not table_exists(con, table):
            con.execute(
                f"create table raw.{table} as "
                f"select *, cast(null as varchar) as _batch_id, cast(null as timestamp) as _loaded_at "
                f"from src.shop.{table} limit 0"
            )
            watermark_from = None
        else:
            max_seen = con.execute(f"select max(updated_at) from raw.{table}").fetchone()[0]
            watermark_from = None if max_seen is None else max_seen - timedelta(hours=LOOKBACK_HOURS)

        where = "" if watermark_from is None else f"where updated_at >= timestamp '{watermark_from:%Y-%m-%d %H:%M:%S}'"
        con.execute(
            f"insert into raw.{table} select *, ?, cast(now() as timestamp) from src.shop.{table} {where}",
            [batch_id],
        )
        rows, max_updated = con.execute(
            f"select count(*), max(updated_at) from raw.{table} where _batch_id = ?", [batch_id]
        ).fetchone()
        con.execute(
            "insert into raw._load_audit values (?, ?, ?, ?, ?, ?, ?)",
            [batch_id, table, watermark_from, rows, max_updated, started, datetime.now()],
        )
        mode = "full" if watermark_from is None else f"since {watermark_from:%Y-%m-%d %H:%M}"
        print(f"[ingest] {table:<12} {rows:>7} rows ({mode})")

    con.close()


if __name__ == "__main__":
    main()
