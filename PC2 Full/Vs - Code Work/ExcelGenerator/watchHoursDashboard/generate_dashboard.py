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
import sys
from datetime import datetime
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
XLSX_PATH           = Path(r"Y:\Veto Logs Backup\Dashboards\OverView\overview_report.xlsx")
HTML_OUT            = Path(r"Y:\Veto Logs Backup\Dashboards\OverView\overview_dashboard.html")
DEVICE_SNAPSHOT_CSV = Path(r"Y:\Veto Logs Backup\Dashboards\OverView\device_snapshot.csv")
DEVICE_DAILY_CSV    = Path(r"Y:\Veto Logs Backup\Dashboards\OverView\device_daily.csv")
# ─────────────────────────────────────────────────────────────────────────────

# Allow path overrides via positional args (original behaviour preserved)
parser = argparse.ArgumentParser(description="Generate Overview Dashboard HTML")
parser.add_argument("xlsx",     nargs="?", default=None, help="Path to overview_report.xlsx")
parser.add_argument("html_out", nargs="?", default=None, help="Output HTML path")
parser.add_argument("--dry-run", action="store_true", help="Parse and validate only — do not write output")
args = parser.parse_args()

if args.xlsx:     XLSX_PATH = Path(args.xlsx)
if args.html_out: HTML_OUT  = Path(args.html_out)

# ── Imports (after path is resolved so lib is importable) ─────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.extract import read_excel
from lib.device  import read_device_data, build_device_summary
from lib.chartjs import get_chartjs
from lib.render  import build_data_blob, render_html

# ── Pipeline ──────────────────────────────────────────────────────────────────
try:
    meta, data_time_range, data_rows = read_excel(XLSX_PATH)
except FileNotFoundError as e:
    print(f"  ERROR: {e}")
    sys.exit(1)

snapshot_rows, device_daily_rows, device_first_seen_counts = read_device_data(
    DEVICE_SNAPSHOT_CSV, DEVICE_DAILY_CSV
)
device_summary = build_device_summary(snapshot_rows, device_daily_rows)

generated_at = datetime.now()
data_blob    = build_data_blob(
    meta, data_rows, device_daily_rows,
    device_first_seen_counts, device_summary,
    data_time_range, generated_at,
)

chartjs = get_chartjs()
html    = render_html(data_blob, chartjs)

# ── Output ────────────────────────────────────────────────────────────────────
if args.dry_run:
    print(f"\n[Dry run] Pipeline OK — {len(data_rows)} data rows, {len(html):,} HTML chars")
    print(f"  Would write to: {HTML_OUT}")
else:
    HTML_OUT.parent.mkdir(parents=True, exist_ok=True)
    HTML_OUT.write_text(html, encoding="utf-8")
    print(f"\n[Success] Dashboard written to: {HTML_OUT}")
