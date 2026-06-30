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


def device_data_span(daily_rows: list[dict]) -> tuple[str, str, int]:
    dates = sorted(
        str(row.get("date", "")).strip()[:10]
        for row in daily_rows
        if str(row.get("date", "")).strip()
    )
    if not dates:
        return "", "", 0
    return dates[0], dates[-1], len(set(dates))


def read_identity_device_data(identity_path: Path) -> tuple[list, list, dict]:
    """Build Overview device rows from the compact identity mart when available."""
    if not identity_path.exists():
        return [], [], {}

    try:
        import pandas as pd
    except ImportError:
        return [], [], {}

    try:
        df = pd.read_parquet(identity_path)
    except (FileNotFoundError, OSError, ValueError):
        return [], [], {}

    required = {"source", "log_date", "device_id"}
    if df.empty or not required.issubset(df.columns):
        return [], [], {}

    work = df.copy()
    work["date"] = work["log_date"].astype(str).str.slice(0, 10)
    work["source"] = work["source"].fillna("stream").astype(str).str.strip().str.lower()
    work["source"] = work["source"].replace("", "stream")
    work["device_id"] = work["device_id"].astype(str).str.strip()
    work = work[
        work["date"].str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)
        & work["device_id"].ne("")
        & work["device_id"].str.lower().ne("nan")
    ]
    if work.empty:
        return [], [], {}

    if "rows_with_identity" in work.columns:
        work["rows_on_date"] = pd.to_numeric(work["rows_with_identity"], errors="coerce").fillna(0)
    else:
        work["rows_on_date"] = 0
    if "distinct_sessions" in work.columns:
        work["distinct_sessions"] = pd.to_numeric(work["distinct_sessions"], errors="coerce").fillna(0)
    else:
        work["distinct_sessions"] = 0

    grouped = work.groupby(["source", "device_id", "date"], as_index=False).agg(
        rows_on_date=("rows_on_date", "sum"),
        distinct_sessions=("distinct_sessions", "sum"),
    )

    device_idx: dict[str, int] = {}
    first_seen_by_device: dict[int, str] = {}
    first_seen_by_source_device: dict[tuple[str, int], str] = {}
    snapshot_map: dict[int, dict] = {}
    daily_map: dict[str, dict] = {}

    for row in grouped.itertuples(index=False):
        source = str(row.source or "stream").strip().lower() or "stream"
        device_id = str(row.device_id)
        date_value = str(row.date)
        idx = device_idx.get(device_id)
        if idx is None:
            idx = len(device_idx)
            device_idx[device_id] = idx

        rows_on_date = int(row.rows_on_date or 0)
        sessions = int(row.distinct_sessions or 0)

        old_first = first_seen_by_device.get(idx)
        if old_first is None or date_value < old_first:
            first_seen_by_device[idx] = date_value
        source_key = (source, idx)
        old_source_first = first_seen_by_source_device.get(source_key)
        if old_source_first is None or date_value < old_source_first:
            first_seen_by_source_device[source_key] = date_value

        snap = snapshot_map.setdefault(
            idx,
            {
                "device_id": device_id,
                "first_seen": date_value,
                "last_seen": date_value,
                "days": set(),
                "total_rows": 0,
                "sessions": 0,
            },
        )
        snap["first_seen"] = min(snap["first_seen"], date_value)
        snap["last_seen"] = max(snap["last_seen"], date_value)
        snap["days"].add(date_value)
        snap["total_rows"] += rows_on_date
        snap["sessions"] += sessions

        item = daily_map.setdefault(
            date_value,
            {"date": date_value, "devices": set(), "rows": 0, "sessions": 0, "sources": {}},
        )
        item["devices"].add(idx)
        item["rows"] += rows_on_date
        item["sessions"] += sessions
        source_item = item["sources"].setdefault(
            source,
            {"devices": set(), "rows": 0, "sessions": 0},
        )
        source_item["devices"].add(idx)
        source_item["rows"] += rows_on_date
        source_item["sessions"] += sessions

    first_seen_counts: dict[str, int] = {}
    snapshot_rows = []
    for idx, snap in snapshot_map.items():
        first_seen = str(snap["first_seen"])
        first_seen_counts[first_seen] = first_seen_counts.get(first_seen, 0) + 1
        snapshot_rows.append(
            {
                "device_id": snap["device_id"],
                "first_seen": first_seen,
                "last_seen": snap["last_seen"],
                "days_seen": len(snap["days"]),
                "total_rows": int(snap["total_rows"]),
                "sessions": int(snap["sessions"]),
            }
        )

    daily_rows = [
        {
            "date": date_value,
            "devices": sorted(value["devices"]),
            "active_devices": len(value["devices"]),
            "new_devices": sum(
                1 for idx in value["devices"] if first_seen_by_device.get(idx) == date_value
            ),
            "rows": int(value["rows"]),
            "sessions": int(value["sessions"]),
            "sources": {
                source: {
                    "devices": sorted(src_value["devices"]),
                    "active_devices": len(src_value["devices"]),
                    "new_devices": sum(
                        1
                        for idx in src_value["devices"]
                        if first_seen_by_source_device.get((source, idx)) == date_value
                    ),
                    "rows": int(src_value["rows"]),
                    "sessions": int(src_value["sessions"]),
                }
                for source, src_value in sorted(value["sources"].items())
            },
        }
        for date_value, value in sorted(daily_map.items())
    ]

    return snapshot_rows, daily_rows, first_seen_counts


def filter_device_rows_to_dates(
    device_daily_rows: list[dict],
    first_seen_counts: dict,
    allowed_dates: set[str],
) -> tuple[list[dict], dict]:
    if not allowed_dates:
        return device_daily_rows, first_seen_counts
    filtered_rows = [
        row
        for row in device_daily_rows
        if str(row.get("date", "")).strip()[:10] in allowed_dates
    ]
    filtered_first_seen = {
        str(date_value): count
        for date_value, count in first_seen_counts.items()
        if str(date_value)[:10] in allowed_dates
    }
    return filtered_rows, filtered_first_seen


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

    completed_dates = {
        str(row.get("date", "")).strip()[:10]
        for row in data_rows
        if str(row.get("date", "")).strip()
    }

    snapshot_rows, device_daily_rows, device_first_seen_counts = read_device_data(
        device_snapshot_csv,
        device_daily_csv,
    )
    identity_device_path = Path(
        os.getenv(
            "VG_OVERVIEW_IDENTITY_DEVICE_DAILY",
            str(base_dir.parent / "identity" / "identity_device_daily.parquet"),
        )
    )
    identity_snapshot_rows, identity_daily_rows, identity_first_seen_counts = read_identity_device_data(
        identity_device_path
    )
    if identity_daily_rows and device_data_span(identity_daily_rows)[2] > device_data_span(device_daily_rows)[2]:
        # Historical lake partitions may be archived off the active lake.
        # The identity mart is the compact, append-safe source for full-range device activity.
        snapshot_rows = identity_snapshot_rows
        device_daily_rows = identity_daily_rows
        device_first_seen_counts = identity_first_seen_counts

    device_daily_rows, device_first_seen_counts = filter_device_rows_to_dates(
        device_daily_rows,
        device_first_seen_counts,
        completed_dates,
    )
    device_summary = build_device_summary(snapshot_rows, device_daily_rows)
    source_rows = read_source_daily(source_daily_csv)
    if completed_dates:
        source_rows = [
            row
            for row in source_rows
            if str(row.get("date", "")).strip()[:10] in completed_dates
        ]
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
