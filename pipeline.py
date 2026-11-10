#!/usr/bin/env python
"""
One cross-platform entry point (works on Windows, macOS, Linux).

    python pipeline.py seed --days 60     # build source history
    python pipeline.py ingest             # Postgres -> DuckDB raw
    python pipeline.py transform          # dbt deps + dbt build (models, snapshots, tests)
    python pipeline.py cycle --n 3        # simulate N new days: generate -> ingest -> transform
    python pipeline.py export             # gold layer -> Parquet for Power BI
    python pipeline.py docs               # dbt docs generate
    python pipeline.py demo               # everything, end to end
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DBT_DIR = ROOT / "dbt_project"
ENV = os.environ.copy()
ENV.setdefault("DUCKDB_PATH", str(ROOT / "warehouse" / "warehouse.duckdb"))
ENV.setdefault("DBT_PROFILES_DIR", str(DBT_DIR))


def run(cmd: list[str], cwd: Path = ROOT) -> None:
    print(f"\n$ {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, cwd=cwd, env=ENV, check=True)


def py(script: str, *args: str) -> None:
    run([sys.executable, str(ROOT / script), *args])


def dbt(*args: str) -> None:
    exe = shutil.which("dbt")
    if exe is None:
        sys.exit("dbt not found. Activate your virtual environment and run: pip install -r requirements.txt")
    run([exe, *args], cwd=DBT_DIR)


def transform() -> None:
    if not (DBT_DIR / "dbt_packages").exists():
        dbt("deps")
    dbt("build")


def cycle(n: int) -> None:
    for i in range(n):
        print(f"\n===== cycle {i + 1}/{n} =====")
        py("generator/generate.py", "batch", "--days", "1")
        py("ingestion/extract_load.py")
        transform()


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    s = sub.add_parser("seed")
    s.add_argument("--days", type=int, default=60)
    sub.add_parser("ingest")
    sub.add_parser("transform")
    c = sub.add_parser("cycle")
    c.add_argument("--n", type=int, default=1)
    sub.add_parser("export")
    sub.add_parser("docs")
    d = sub.add_parser("demo")
    d.add_argument("--days", type=int, default=45)
    d.add_argument("--cycles", type=int, default=5)
    args = parser.parse_args()

    if args.command == "seed":
        py("generator/generate.py", "seed", "--days", str(args.days))
    elif args.command == "ingest":
        py("ingestion/extract_load.py")
    elif args.command == "transform":
        transform()
    elif args.command == "cycle":
        cycle(args.n)
    elif args.command == "export":
        py("serving/export_marts.py")
    elif args.command == "docs":
        dbt("docs", "generate")
        print("\nRun `dbt docs serve` inside dbt_project/ to browse the docs.")
    elif args.command == "demo":
        # start from a clean warehouse so snapshots and raw history match the fresh source
        for suffix in ("", ".wal"):
            Path(ENV["DUCKDB_PATH"] + suffix).unlink(missing_ok=True)
        py("generator/generate.py", "seed", "--days", str(args.days))
        py("ingestion/extract_load.py", "--full-refresh")
        dbt("deps")
        dbt("build", "--full-refresh")
        cycle(args.cycles)
        py("serving/export_marts.py")
        print("\nDemo complete. Warehouse:", ENV["DUCKDB_PATH"])


if __name__ == "__main__":
    main()
