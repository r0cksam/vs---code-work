#!/usr/bin/env python3
"""
generate_watch_hours.py — Entry point (refactored)
───────────────────────────────────────────────────
Usage:
    python generate_watch_hours.py
    python generate_watch_hours.py --profile <dir> --out <file> --title <str>
    python generate_watch_hours.py --dry-run    # validate pipeline without writing

Improvements over original:
    - Modular: constants / utils / readers / aggregations / report / chartjs / render
    - All bare except: replaced with typed (ValueError, TypeError, Exception)
    - sn() / numeric() renamed to coerce_float() / numeric() — self-documenting
    - Chart.js cache path uses __file__.resolve() — safe from cwd differences
    - HTML template in template.html; no 600-line embedded strings
    - _aggCache: all JS-side per-table aggregators cached per date window,
      invalidated only on filter change (debounced 200ms)
    - escapeHtml() added throughout JS table rendering — XSS guard
    - --dry-run flag for CI/testing without writing output
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DEFAULT_PROFILE_DIR = ROOT.parent / "vglive_channel_profile" / "deep_profile_full"
DEFAULT_OUT = Path(r"Y:\Veto Logs Backup\Dashboards\Watch Hours\veto_watch_hours.html")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a standalone VgLive watch-hours HTML report.")
    parser.add_argument("--profile", default=str(DEFAULT_PROFILE_DIR), help="Folder containing deep profile CSV files.")
    parser.add_argument("--out",     default=str(DEFAULT_OUT),          help="Output HTML file path.")
    parser.add_argument("--title",   default="Veto Watch Hours",        help="HTML report title.")
    parser.add_argument("--dry-run", action="store_true",               help="Parse and validate only — do not write output.")
    args = parser.parse_args()

    profile_dir = Path(args.profile).resolve()
    out_path    = Path(args.out).resolve()

    if not profile_dir.exists():
        raise SystemExit(f"Profile folder not found: {profile_dir}")

    # Lazy imports so argparse --help works without pandas installed
    from lib.report  import build_report_data
    from lib.chartjs import load_chartjs
    from lib.render  import render_html

    print(f"Building report from: {profile_dir}")
    data    = build_report_data(profile_dir, args.title)
    chartjs = load_chartjs()
    html    = render_html(data, chartjs)

    if args.dry_run:
        print(f"\n[Dry run] Pipeline OK — {len(html):,} HTML chars")
        print(f"  Would write to: {out_path}")
    else:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(html, encoding="utf-8")
        print(f"\nReport written: {out_path}")
        print(f"Size: {out_path.stat().st_size / 1024:.1f} KB")


if __name__ == "__main__":
    main()
