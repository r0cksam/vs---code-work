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
import os
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
ETL_ROOT = ROOT.parent.parent
SRC_ROOT = ETL_ROOT / "src"
PROFILE_ROOT = SRC_ROOT / "profile"
for path in [ETL_ROOT, SRC_ROOT, PROFILE_ROOT]:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

def _resolve_profile_dir() -> Path:
    env_profile = os.getenv("VG_DASH_PROFILE_DIR")
    if env_profile:
        return Path(env_profile).expanduser().resolve()

    for candidate in [
        ETL_ROOT / "output" / "watch_hours" / "profile",
    ]:
        if candidate.exists():
            return candidate

    # Prefer profile inside this ETL folder; callers can override with --profile.
    for base in [Path.cwd()] + list(Path.cwd().parents) + [ROOT] + list(ROOT.parents):
        candidate = base / "watch_hours" / "profile"
        if candidate.exists():
            return candidate

    return ETL_ROOT / "output" / "watch_hours" / "profile"


DEFAULT_PROFILE_DIR = _resolve_profile_dir()
DEFAULT_OUT = Path(os.getenv("VG_DASH_WATCH_OUT", ETL_ROOT / "output" / "watch_hours" / "veto_watch_hours.html"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a standalone Veto watch-hours HTML report.")
    parser.add_argument("--profile", default=str(DEFAULT_PROFILE_DIR), help="Folder containing watch-hours profile tables.")
    parser.add_argument("--out",     default=str(DEFAULT_OUT),          help="Output HTML file path.")
    parser.add_argument("--title",   default="Veto Watch Hours",        help="HTML report title.")
    parser.add_argument("--dry-run", action="store_true",               help="Parse and validate only — do not write output.")
    args = parser.parse_args()

    profile_dir = Path(args.profile).resolve()
    out_path    = Path(args.out).resolve()

    if not profile_dir.exists():
        raise SystemExit(f"Profile folder not found: {profile_dir}")

    # Lazy imports so argparse --help works without pandas installed
    from common.chartjs import load_chartjs
    from common.render import json_blob, render_template
    from lib.constants import CHARTJS_CACHE
    from lib.report  import build_report_data
    from lib.readers import missing_profile_inputs

    print(f"Building report from: {profile_dir}")
    missing = missing_profile_inputs(profile_dir)
    if missing:
        preview = ", ".join(missing[:12])
        more = f" and {len(missing) - 12} more" if len(missing) > 12 else ""
        raise SystemExit(
            "Watch-hours profile is incomplete. Run the deep-profile step first. "
            f"Missing: {preview}{more}"
        )

    data    = build_report_data(profile_dir, args.title)
    chartjs = load_chartjs(CHARTJS_CACHE, fallback="window.Chart=null;")
    html    = render_template(HERE / "template.html", DATA_BLOB=json_blob(data), CHARTJS=chartjs or "window.Chart=null;")

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

