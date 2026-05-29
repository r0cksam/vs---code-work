# pDash Modular v2

Production-style modular version of your Parquet Explorer.

## Run

```bash
cd pdash_modular_v2
pip install -r requirements.txt
streamlit run app.py
```

## What changed in v2

- Central Query Engine layer using DuckDB first, PyArrow streaming fallback
- Metadata index for files, row counts, schemas, and file-level columns
- Config layer in `pdash/config.py`
- Central logging to `.pdash_logs/pdash.log`
- Safer large-export warning
- Faster full-dataset unique values and chart aggregations
- Cleaner module split while keeping your existing Streamlit workflow

## Main files

- `app.py` — Streamlit UI entrypoint
- `pdash/core/metadata.py` — parquet inventory/schema index
- `pdash/core/query_engine.py` — all heavy query execution
- `pdash/core/data.py` — compatibility API used by app.py
- `pdash/core/sql.py` — safe SQL helpers
- `pdash/core/visualization.py` — chart recommendation helpers
- `pdash/analytics/behavior.py` — user/global behavior analytics
- `pdash/config.py` — tunable performance defaults

## Tune performance

Use environment variables before running Streamlit:

```bash
export PDASH_DUCKDB_THREADS=8
export PDASH_BATCH_SIZE=200000
export PDASH_EXPORT_WARNING_ROWS=1000000
streamlit run app.py
```

## v3 Global Behavior preload changes

Global Behavior now:
- finds the first and last available behavior record date automatically
- defaults the dashboard to that full date range
- preloads Content + Channel analytics once into a cached bundle
- auto-loads Pure Channel Master
- avoids rerunning heavy parquet queries when you switch Focus, Top N, or section
- provides a manual **Reload Global Cache** button when you actually want to recompute

Run:

```bash
streamlit run app.py
```
