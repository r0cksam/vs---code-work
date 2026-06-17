"""
generate_dashboard.py - Entry point for the Overview Dashboard.

Usage:
    python generate_dashboard.py
    python generate_dashboard.py <xlsx_path> <html_out>
    python generate_dashboard.py --dry-run
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import sys
import types
from datetime import datetime
from pathlib import Path
from typing import Any


HERE = Path(__file__).resolve().parent


def resolve_src_root() -> Path:
    env = os.getenv("VG_ETL_SRC_ROOT")
    candidates = [Path(env).expanduser().resolve()] if env else []
    candidates.extend(list(Path(__file__).resolve().parents[:6]))
    for c in candidates:
        if (c / "common" / "chartjs.py").exists() and (c / "common" / "render.py").exists():
            return c
    raise FileNotFoundError(
        "Cannot find ETL src/common. Set VG_ETL_SRC_ROOT env var."
    )


SRC_ROOT = resolve_src_root()
ETL_ROOT = SRC_ROOT.parent
DEFAULT_CHARTJS_CACHE = ETL_ROOT / "output" / "cache" / "chartjs" / "chart.umd.min.js"
BASE_DIR = Path(os.getenv("VG_DASH_OVERVIEW_BASE", str(ETL_ROOT / "output" / "overview")))
XLSX_PATH = BASE_DIR / "overview_report.xlsx"
HTML_OUT = BASE_DIR / "overview_dashboard.html"
DEVICE_SNAPSHOT_CSV = BASE_DIR / "device_snapshot.csv"
DEVICE_DAILY_CSV = BASE_DIR / "device_daily.csv"
SOURCE_DAILY_CSV = BASE_DIR / "overview_source_daily.csv"


def _load_module(module_name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load {module_name} from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _ensure_package(package_name: str, package_dir: Path) -> None:
    if package_name in sys.modules:
        return
    package = types.ModuleType(package_name)
    package.__path__ = [str(package_dir)]  # type: ignore[attr-defined]
    sys.modules[package_name] = package


_chartjs_module = _load_module("veto_common_chartjs", SRC_ROOT / "common" / "chartjs.py")
_source_ranges_module = _load_module("veto_common_source_ranges", SRC_ROOT / "common" / "source_ranges.py")
_render_module = _load_module("veto_common_render", SRC_ROOT / "common" / "render.py")
_ensure_package("veto_overview_lib", HERE / "lib")
_extract_module = _load_module("veto_overview_lib.extract", HERE / "lib" / "extract.py")
_device_module = _load_module("veto_overview_lib.device", HERE / "lib" / "device.py")

load_chartjs = _chartjs_module.load_chartjs
combined_range = _source_ranges_module.combined_range
true_source_ranges_from_lake = _source_ranges_module.true_source_ranges_from_lake
chartjs_script = _render_module.chartjs_script
json_blob = _render_module.json_blob
render_template = _render_module.render_template
read_device_data = _device_module.read_device_data
build_device_summary = _device_module.build_device_summary
read_excel = _extract_module.read_excel
read_source_daily = _extract_module.read_source_daily


def atomic_write_text(path: Path, text: str, encoding: str = "utf-8") -> None:
    import tempfile

    temp_path = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
        os.close(fd)
        temp_path = Path(tmp)
        temp_path.write_text(text, encoding=encoding)
        os.replace(temp_path, path)
    finally:
        if temp_path is not None and temp_path.exists():
            temp_path.unlink()


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Overview Dashboard HTML")
    parser.add_argument(
        "--data-dir",
        default=None,
        help="Folder containing overview_report.xlsx, device_snapshot.csv, device_daily.csv, and output html",
    )
    parser.add_argument("xlsx", nargs="?", default=None, help="Path to overview_report.xlsx")
    parser.add_argument("html_out", nargs="?", default=None, help="Output HTML path")
    parser.add_argument("--dry-run", action="store_true", help="Parse and validate only - do not write output")
    args = parser.parse_args()

    base_dir = Path(args.data_dir).resolve() if args.data_dir else BASE_DIR
    xlsx_path = Path(args.xlsx) if args.xlsx else base_dir / "overview_report.xlsx"
    html_out = Path(args.html_out) if args.html_out else base_dir / "overview_dashboard.html"
    device_snapshot_csv = base_dir / "device_snapshot.csv"
    device_daily_csv = base_dir / "device_daily.csv"
    source_daily_csv = base_dir / "overview_source_daily.csv"

    try:
        meta, data_time_range, data_rows = read_excel(xlsx_path)
    except FileNotFoundError as exc:
        raise SystemExit(f"ERROR: {exc}") from exc

    missing_device_files = [
        str(path)
        for path in [device_snapshot_csv, device_daily_csv]
        if not path.exists()
    ]
    if missing_device_files:
        print("  WARNING: device CSV input missing; device panels will be incomplete.")
        for missing in missing_device_files:
            print(f"    - {missing}")

    snapshot_rows, device_daily_rows, device_first_seen_counts = read_device_data(
        device_snapshot_csv,
        device_daily_csv,
    )
    device_summary = build_device_summary(snapshot_rows, device_daily_rows)
    source_rows = read_source_daily(source_daily_csv)
    source_dates: dict[str, list[str]] = {}
    for row in source_rows:
        source = str(row.get("source", "")).strip().lower()
        date_value = str(row.get("date", "")).strip()[:10]
        if source and date_value:
            try:
                datetime.strptime(date_value, "%Y-%m-%d")
                source_dates.setdefault(source, []).append(date_value)
            except ValueError:
                pass
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
        "device_data_missing": bool(missing_device_files),
        "device_missing_files": missing_device_files,
    })

    if args.dry_run:
        print(f"\n[Dry run] Pipeline OK - {len(data_rows)} data rows")
        print(f"  Would write to: {html_out}")
    else:
        chartjs = load_chartjs(DEFAULT_CHARTJS_CACHE, fallback=None)
        html = render_template(
            HERE / "template.html",
            DATA_BLOB=data_blob,
            CHARTJS_TAG=chartjs_script(chartjs),
        )
        atomic_write_text(html_out, html)
        print(f"\n[Success] Dashboard written to: {html_out}")


if __name__ == "__main__":
    main()
