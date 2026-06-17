"""
generate_dashboard.py — Entry point (Enterprise Edition, refactored)
─────────────────────────────────────────────────────────────────────
Usage:
    python generate_dashboard.py
    python generate_dashboard.py <xlsx_path> <html_out>
    python generate_dashboard.py --dry-run          # validate pipeline, no file written

Improvements over original:
    - Modular: extract / device / chartjs / render split into lib/
    - coerce_float() replaces cryptic sn(); typed except (ValueError, TypeError)
    - parse_date() isolated and testable; no repeated format-loop inline
    - Chart.js cache path uses __file__.resolve() — safe from cwd changes
    - HTML template in template.html; Python no longer embeds 600-line strings
    - scaleStats cached per view-window; invalidated only on filter change
    - Chart updates batched with ch.update('none') — skips redundant animation
    - Date inputs debounced 200 ms — no re-render on every keypress
    - --dry-run flag for CI/testing without writing output
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
HERE                = Path(__file__).resolve().parent
SRC_ROOT            = HERE.parents[1]
ETL_ROOT            = SRC_ROOT.parent
BASE_DIR            = Path(os.getenv("VG_DASH_OVERVIEW_BASE", str(ETL_ROOT / "output" / "overview")))
XLSX_PATH           = BASE_DIR / "overview_report.xlsx"
HTML_OUT            = BASE_DIR / "overview_dashboard.html"
DEVICE_SNAPSHOT_CSV = BASE_DIR / "device_snapshot.csv"
DEVICE_DAILY_CSV    = BASE_DIR / "device_daily.csv"
SOURCE_DAILY_CSV    = BASE_DIR / "overview_source_daily.csv"
# ─────────────────────────────────────────────────────────────────────────────

# Allow path overrides via positional args (original behaviour preserved)
parser = argparse.ArgumentParser(description="Generate Overview Dashboard HTML")
parser.add_argument("--data-dir", default=None, help="Folder containing overview_report.xlsx, device_snapshot.csv, device_daily.csv, and output html")
parser.add_argument("xlsx",     nargs="?", default=None, help="Path to overview_report.xlsx")
parser.add_argument("html_out", nargs="?", default=None, help="Output HTML path")
parser.add_argument("--dry-run", action="store_true", help="Parse and validate only — do not write output")
args = parser.parse_args()
base_dir = Path(args.data_dir).resolve() if args.data_dir else BASE_DIR
if args.xlsx:     XLSX_PATH = Path(args.xlsx)
else:             XLSX_PATH = base_dir / "overview_report.xlsx"
if args.html_out: HTML_OUT  = Path(args.html_out)
else:             HTML_OUT  = base_dir / "overview_dashboard.html"
DEVICE_SNAPSHOT_CSV = base_dir / "device_snapshot.csv"
DEVICE_DAILY_CSV    = base_dir / "device_daily.csv"
SOURCE_DAILY_CSV    = base_dir / "overview_source_daily.csv"

# Imports after paths are resolved so local dashboard libs and ETL common helpers are importable.
for path in [HERE, SRC_ROOT]:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from common.chartjs import load_chartjs
from common.source_ranges import combined_range, true_source_ranges_from_lake
from common.render import chartjs_script, json_blob, render_template
from lib.device  import read_device_data, build_device_summary
from lib.extract import read_excel, read_source_daily


try:
    meta, data_time_range, data_rows = read_excel(XLSX_PATH)
except FileNotFoundError as exc:
    print(f"  ERROR: {exc}")
    sys.exit(1)

missing_device_files = [
    str(path)
    for path in [DEVICE_SNAPSHOT_CSV, DEVICE_DAILY_CSV]
    if not path.exists()
]
if missing_device_files:
    print("  WARNING: device CSV input missing; device panels will be incomplete.")
    for missing in missing_device_files:
        print(f"    - {missing}")

snapshot_rows, device_daily_rows, device_first_seen_counts = read_device_data(
    DEVICE_SNAPSHOT_CSV,
    DEVICE_DAILY_CSV,
)
device_summary = build_device_summary(snapshot_rows, device_daily_rows)
source_rows = read_source_daily(SOURCE_DAILY_CSV)
source_dates: dict[str, list[str]] = {}
for row in source_rows:
    source = str(row.get("source", "")).strip().lower()
    date_value = str(row.get("date", "")).strip()[:10]
    if source and date_value:
        source_dates.setdefault(source, []).append(date_value)
lake_root = Path(os.getenv("VG_ETL_LAKE_ROOT", str(ETL_ROOT / "data" / "lake"))).expanduser().resolve()
source_true_ranges = true_source_ranges_from_lake(source_dates, lake_root)
true_data_range = combined_range(source_true_ranges)

generated_at = datetime.now()
data_blob = json_blob({
    "meta": meta,
    "rows": data_rows,
    "source_rows": source_rows,
    "device_daily": device_daily_rows,
    "device_first_seen_counts": device_first_seen_counts,
    "device_summary": device_summary,
    "generated": generated_at.strftime("%Y-%m-%d %H:%M:%S"),
    "report_date": generated_at.strftime("%Y-%m-%d"),
    "data_range": data_time_range,
    "true_data_range": true_data_range,
    "source_true_ranges": source_true_ranges,
})

ETL_ROOT = HERE.parents[2]
chartjs = load_chartjs(ETL_ROOT / "output" / "cache" / "chartjs" / "chart.umd.min.js", fallback=None)
html = render_template(
    HERE / "template.html",
    DATA_BLOB=data_blob,
    CHARTJS_TAG=chartjs_script(chartjs),
)

if args.dry_run:
    print(f"\n[Dry run] Pipeline OK - {len(data_rows)} data rows, {len(html):,} HTML chars")
    print(f"  Would write to: {HTML_OUT}")
else:
    HTML_OUT.parent.mkdir(parents=True, exist_ok=True)
    HTML_OUT.write_text(html, encoding="utf-8")
    print(f"\n[Success] Dashboard written to: {HTML_OUT}")
