#!/usr/bin/env python
"""Export the gold layer (marts schema) to Parquet for Power BI / any BI tool."""
from __future__ import annotations

import os
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DUCKDB_PATH = os.getenv("DUCKDB_PATH", str(ROOT / "warehouse" / "warehouse.duckdb"))
EXPORT_DIR = ROOT / "warehouse" / "exports"


def main() -> None:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    tables = [r[0] for r in con.execute(
        "select table_name from information_schema.tables where table_schema = 'marts' order by 1"
    ).fetchall()]
    for table in tables:
        target = (EXPORT_DIR / f"{table}.parquet").as_posix()
        con.execute(f"copy (select * from marts.{table}) to '{target}' (format parquet)")
        print(f"[export] marts.{table} -> {target}")
    con.close()


if __name__ == "__main__":
    main()
