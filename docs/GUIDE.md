# End-to-End Build Guide: Supply Chain Analytics Lakehouse

This guide takes you from an empty laptop to a published GitHub project you can defend in interviews.
Plan for **4 weeks at about 1–1.5 hours a day**. Each phase ends with a checkpoint and a commit.

> **Golden rule:** don't just copy the files. Read each one, run it, break it on purpose and fix it.
> Interviewers will ask *why* you made each choice. The "Interview angle" notes in each phase are what you should be able to explain without notes.

---

## Phase 0 — Set up your machine (Day 1)

Install:

| Tool | Why | Check |
|---|---|---|
| Python 3.11 | Pipeline code, dbt | `python --version` |
| Docker Desktop | Runs the Postgres source database | `docker --version` |
| Git | Version control | `git --version` |
| VS Code + extensions: *Python*, *dbt Power User*, *SQLFluff*, *YAML* | Editor | – |
| DBeaver (optional) | Browse Postgres and DuckDB visually | – |
| GitHub account | Publishing | – |

Windows tips:
- Use **PowerShell** (or Git Bash). Activate a virtual environment with `.venv\Scripts\activate`.
- If activation is blocked: `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`.
- Enable WSL 2 when Docker Desktop asks.

---

## Phase 1 — Create the repository (Day 1)

1. On GitHub create an empty repo named **`supply-chain-lakehouse`** (public, no README).
2. Unzip the starter project and open the folder in VS Code.
3. In the terminal:

```bash
python -m venv .venv
.venv\Scripts\activate            # macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
git init
git add .
git commit -m "chore: project scaffold"
git branch -M main
git remote add origin https://github.com/<your-username>/supply-chain-lakehouse.git
git push -u origin main
```

4. In `README.md`, replace `<your-github-username>` in the badge and clone URL.

**Tip for a better commit history:** instead of pushing everything at once, commit phase by phase (scaffold → source → ingestion → staging → marts → tests → orchestration → CI → dashboard). A real, gradual history looks much stronger than one giant commit.

✅ **Checkpoint:** repo visible on GitHub.

---

## Phase 2 — Source system: Postgres + data simulator (Days 2–4)

**What you're building:** an operational "Order Management System" database that keeps changing, so your pipeline has to deal with incremental loads, updates, late data and bad data.

```bash
docker compose up -d                    # starts Postgres on localhost:5432
python pipeline.py seed --days 60       # creates schema + 60 simulated days of history
```

Look at the data (DBeaver, or `docker exec -it supply_source_db psql -U de_user -d supply`):

```sql
select order_status, count(*) from shop.orders group by 1;
select * from shop.shipments order by updated_at desc limit 10;
select * from shop._sim_clock;            -- current simulated date
```

Advance time by a day and watch rows change:

```bash
python generator/generate.py batch --days 1
```

**Read and understand** `source_db/schema.sql` and `generator/generate.py`:
- Every table has `created_at` / `updated_at` → this is what incremental extraction relies on.
- The simulator injects **late-arriving records** (3% of shipments back-dated a day), **bad records** (negative freight, delivered-before-shipped), **no-op updates**, and a **carrier incident** (Coastal Freight delays on days 10–17 of each month).

**Interview angle:** *"Why simulate instead of using a static Kaggle CSV?"* → A static file can't exercise incremental loading, CDC-like updates, SCD2 or late data. The simulator lets me reproduce production problems deterministically (seeded RNG).

✅ **Checkpoint:** `select count(*) from shop.orders` returns ~2,500+. Commit: `feat: source OMS schema and simulator`.

---

## Phase 3 — Incremental ingestion to the raw layer (Days 5–7)

```bash
python pipeline.py ingest      # first run = full load
python generator/generate.py batch --days 1
python pipeline.py ingest      # second run = incremental (look at the row counts)
```

Inspect:

```bash
python -c "import duckdb; c=duckdb.connect('warehouse/warehouse.duckdb'); print(c.sql('select * from raw._load_audit order by started_at desc limit 14'))"
```

**Concepts to master** (`ingestion/extract_load.py`):
1. **High-watermark:** read only rows with `updated_at >= max(updated_at) already loaded`.
2. **Lookback window (48h):** deliberately re-read recent rows so back-dated (late) records aren't missed. This creates duplicates in raw, which is intended.
3. **Append-only raw + lineage columns** (`_batch_id`, `_loaded_at`): you never lose history, and every row is traceable to a load.
4. **Audit table:** observability for every load.

**Experiment:** set `INGEST_LOOKBACK_HOURS=0`, run a few cycles, then count shipments in Postgres vs. raw. You'll see late records go missing. That's your interview story.

**Interview angle:** *"Watermark vs. CDC?"* → Watermarks are simple but miss hard deletes and intermediate versions. Log-based CDC (Debezium reading the Postgres WAL) captures everything. Know both and when you'd choose each.

✅ **Checkpoint:** audit table shows full load then incremental loads. Commit: `feat: incremental extract-load with watermark and audit`.

---

## Phase 4 — dbt staging layer (Days 8–10)

```bash
cd dbt_project
dbt deps
dbt debug                     # confirms the DuckDB connection
dbt build --select staging
```

(Set `DBT_PROFILES_DIR` to the `dbt_project` folder, or just use `python pipeline.py transform` from the repo root, which sets it for you.)

**What staging does:** one model per source table. It **dedupes** with
`qualify row_number() over (partition by <key> order by updated_at desc, _loaded_at desc) = 1`, cleans types and names, and in `stg_shipments` adds a `dq_issue` flag.

**Learn:** `_sources.yml` (sources and freshness), `_staging.yml` (tests), `macros/generate_schema_name.sql` (clean schema names).

**Interview angle:** *"How do you handle duplicates?"* → Raw is append-only and may contain duplicates by design; staging picks the latest version per key deterministically (`_loaded_at` tie-break).

✅ **Checkpoint:** `dbt build --select staging` all green. Commit.

---

## Phase 5 — SCD Type 2 and the star schema (Days 11–15)

```bash
dbt build
```

### 5a. SCD Type 2, two ways
- **`int_customer_versions` → `dim_customer` (hand-written):** builds history from the raw change log with `lag()` / `lead()`, and collapses no-op changes using an **attribute hash**. Fully rebuildable.
- **`snap_sellers` → `dim_seller` (dbt snapshot):** captures changes each time dbt runs. Simpler, but history only exists from the first snapshot.

Run a few cycles and look at history:

```bash
cd ..
python pipeline.py cycle --n 3
python -c "import duckdb; c=duckdb.connect('warehouse/warehouse.duckdb'); print(c.sql('select customer_id, city, valid_from, valid_to, is_current from marts.dim_customer where customer_id in (select customer_id from marts.dim_customer group by 1 having count(*)>1) order by 1,3 limit 20'))"
```

### 5b. Facts
- **`fact_order_items`** and **`fact_shipments`** are **incremental** (`delete+insert`) with a **3-day lookback** (`var('incremental_lookback_days')`).
- **Point-in-time joins:** `order_ts >= valid_from and order_ts < valid_to` attach the customer version valid *when the order was placed*.
- **Unknown member `-1`:** facts never silently drop rows.
- `fact_shipments` post-hook removes rows that later failed DQ rules.

### 5c. Analytical marts (your advanced SQL showcase)
- `mart_carrier_performance_monthly`: window functions (rolling 3-month average, `lag` MoM change, `rank` per month), with an enforced **contract**.
- `mart_carrier_delay_streaks`: the **gaps-and-islands** pattern (`date - row_number()`). You'll see the injected Coastal Freight incident show up as streaks.
- `mart_daily_kpis`: a date spine so days with no activity show zeros instead of gaps.

**Interview angle:** be ready to whiteboard gaps-and-islands and SCD2 point-in-time joins. These questions come up often.

✅ **Checkpoint:** `dbt build` green; streaks visible for Coastal Freight. Commit.

---

## Phase 6 — Testing, contracts, data quality (Days 16–18)

Already included; run and **read** them:

```bash
cd dbt_project
dbt test
dbt test --select test_type:unit       # unit test on on-time logic
dbt test --select test_type:singular   # SCD2 integrity, reconciliation, quarantine rate
```

**Break things on purpose:**
1. In `int_shipments_enriched.sql` change `<=` to `<` in `is_on_time`. The unit test fails. Revert.
2. Change a `data_type` in `_marts.yml` for the contract mart. The build fails before writing data. Revert.
3. Look at `quality.quarantine_shipments`. These are the injected bad rows.

**Interview angle:** *"How do you know your incremental model is correct?"* → The reconciliation test compares the fact to staging on every run; SCD2 tests check for overlapping windows and exactly one current row.

✅ **Checkpoint:** all tests pass (the quarantine test may warn, which is expected). Commit.

---

## Phase 7 — Orchestration with Dagster (Days 19–21)

```bash
pip install -r requirements-orchestration.txt
dagster dev -f orchestration/definitions.py
```

Open http://localhost:3000 → **Assets** → see lineage from `oms_activity` → `raw/*` → every dbt model. Click **Materialize all**. Then look at **Jobs → daily_pipeline** and **Schedules** (06:00 IST).

**Interview angle:** *"Why asset-based orchestration?"* → Lineage is first-class; dbt models become assets automatically, and raw asset keys match dbt sources so the graph is end to end.

(If you prefer Airflow, that's fine. Note the choice under Design decisions in the README.)

✅ **Checkpoint:** a full materialization succeeds in the UI. Take a screenshot of the asset graph for the README. Commit.

---

## Phase 8 — CI/CD and published docs (Days 22–23)

1. First, clean SQL style locally so CI is green:
   ```bash
   sqlfluff lint dbt_project/models
   sqlfluff fix dbt_project/models      # auto-fix, then review the diff
   ```
   If a DuckDB-specific line can't be parsed, either rewrite it or add `-- noqa` at the end of that line.
2. Push to GitHub. **Actions** runs `ci.yml`: lint → seed Postgres service → full build → incremental build → freshness.
3. Enable docs: **Settings → Pages → Source: GitHub Actions**. `docs.yml` publishes dbt docs on every push to `main`. Add the Pages URL to your README.
4. Protect `main`: **Settings → Branches → require status checks to pass**. From now on, work through feature branches and pull requests. That's how real teams work, and reviewers notice.

✅ **Checkpoint:** green CI badge in README; dbt docs live on GitHub Pages.

---

## Phase 9 — Dashboard in Power BI (Days 24–25)

```bash
python pipeline.py export        # writes warehouse/exports/*.parquet
```

In Power BI Desktop: **Get Data → Parquet** for each file (or **Folder** for all of them).
- Relationships: `mart_daily_kpis[date_key]`, plus the facts to dims if you import them.
- Suggested pages:
  1. **Executive overview:** orders, GMV, on-time %, avg delay (cards plus a daily trend line).
  2. **Carrier scorecard:** on-time % vs. SLA target by month, rank and rolling trend (from `mart_carrier_performance_monthly`).
  3. **Incidents:** delay streaks timeline (from `mart_carrier_delay_streaks`).
- Save the `.pbix` under `dashboards/` and add screenshots to `docs/images/`.

This is where your BI experience stands out: most data engineering portfolios stop at tables.

✅ **Checkpoint:** screenshots in README. Commit.

---

## Phase 10 — Polish and publish (Days 26–28)

- [ ] README: replace placeholders, fill in the **Results** section with real numbers (row counts from `raw._load_audit`, build times from dbt logs, test count from `dbt test` output).
- [ ] Add the architecture diagram (renders from Mermaid automatically), the Dagster screenshot and the dashboard screenshots.
- [ ] Record a 60–90s demo GIF (ScreenToGif, free) running `python pipeline.py cycle --n 1` and showing the dashboard.
- [ ] Add repo **topics**: `data-engineering`, `dbt`, `duckdb`, `dagster`, `sql`, `scd2`, `elt`, `github-actions`.
- [ ] Pin the repo on your GitHub profile.
- [ ] LinkedIn post: problem → architecture image → 3 lessons learned → link.

---

## Make it your own (pick 1–2 extensions)

These are what make the project clearly yours rather than a template:

| Extension | Signal |
|---|---|
| Swap DuckDB for **Snowflake** (free trial) with `merge` strategy and clustering | Cloud warehouse experience |
| **Debezium + Redpanda** log-based CDC instead of watermarks | Streaming / CDC |
| **Elementary** data-observability report | Data reliability |
| **Slim CI** (`dbt build --select state:modified+ --defer`) | Mature CI/CD |
| Handle **hard deletes** (soft-delete flag in source + `is_deleted` in dims) | Edge-case thinking |
| A **semantic layer / metrics** definition for on-time % | Modern analytics engineering |

---

## Resume bullets (use your measured numbers)

**Supply Chain Analytics Lakehouse** | Python, SQL, dbt, DuckDB, Postgres, Dagster, GitHub Actions, Power BI | github.com/<you>/supply-chain-lakehouse

- Built an end-to-end ELT platform that incrementally ingests **[X]K+** orders and shipments from Postgres into a medallion (raw/staging/marts) warehouse, using high-watermark extraction with a lookback window to recover late-arriving records.
- Designed a star schema with **SCD Type 2** dimensions implemented two ways (window-function SQL over an append-only change log and dbt snapshots), with point-in-time fact joins for historically accurate reporting.
- Implemented **[N]** automated data-quality checks (unit tests, data contracts, reconciliation and SCD2-integrity tests, quarantine of invalid records), enforced in a GitHub Actions CI pipeline on every pull request.
- Delivered carrier SLA analytics (rolling on-time %, gaps-and-islands delay-streak detection) orchestrated daily with Dagster and visualized in Power BI.

---

## Interview questions you should be able to answer

1. Walk me through your architecture. Why each layer?
2. How does your incremental load work? What happens if a job fails halfway? *(append-only + watermark from data, not from a state file → safe re-run)*
3. How do you handle late-arriving data, both in ingestion and in incremental models?
4. Explain SCD Type 2. How did you build it, and when would you use a snapshot instead?
5. What is a point-in-time join? What breaks if a dimension change arrives late? *(needs re-processing / full refresh)*
6. Explain your gaps-and-islands query.
7. `delete+insert` vs. `merge` vs. `append`: when do you use each?
8. How do you test data pipelines? Unit test vs. data test vs. contract?
9. What wouldn't scale here, and what would you change for 1B rows? *(cloud warehouse, partitioning/clustering, CDC, parallel extraction, file-based landing zone)*
10. How would you handle hard deletes in the source?

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `port 5432 already in use` | A local Postgres is running. Set `PG_PORT=5433` in your shell and in `docker-compose` (it reads `PG_PORT`). |
| `Could not set lock on file ... warehouse.duckdb` | Another process (DBeaver, Python shell, Dagster) has the file open. Close it; DuckDB allows a single writer. |
| `dbt not found` | Activate the virtual environment. |
| `Source database is empty` | Run `python pipeline.py seed` first. |
| Extension download fails on ingest | DuckDB downloads the `postgres` extension on first use and needs internet access once. |
| Contract error on `mart_carrier_performance_monthly` | The SQL `cast(...)` types must match `data_type` in `_marts.yml`. |
| Weird history after re-seeding | Re-seeding resets source IDs. Run `python pipeline.py demo`, which deletes the warehouse and rebuilds cleanly. |
| SQLFluff parse error on a DuckDB-specific line | Add `-- noqa` to that line, or exclude the rule in `.sqlfluff`. |

---

## 4-week schedule at a glance

| Week | Days | Outcome |
|---|---|---|
| 1 | 1–7 | Machine set up, repo live, source simulator running, incremental ingestion with audit |
| 2 | 8–15 | Staging, SCD2 (both ways), facts, analytical marts |
| 3 | 16–21 | Tests, contracts, unit tests, Dagster orchestration |
| 4 | 22–28 | CI/CD, dbt docs on Pages, Power BI dashboard, README polish, publish and share |
