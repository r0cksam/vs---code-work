#!/usr/bin/env python3
import argparse
import json
import shutil
from pathlib import Path


OVERVIEW_MARKER = "const {meta,rows,source_rows,device_daily,device_first_seen_counts,device_summary,generated,report_date,data_range} = "
WATCH_MARKER = "const D = "


def embedded_json(path: Path, marker: str) -> dict:
    text = path.read_text(encoding="utf-8")
    start = text.index(marker) + len(marker)
    value, _ = json.JSONDecoder().raw_decode(text[start:])
    return value


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=True, separators=(",", ":")),
        encoding="utf-8",
    )
    temporary.replace(path)


def copy_overview_files(source: Path, destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    for name in (
        "overview_report.xlsx",
        "overview_source_daily.csv",
        "device_daily.csv",
        "device_snapshot.csv",
    ):
        src = source / name
        if src.exists():
            shutil.copy2(src, destination / name)
            print(f"  Copied {src.name}")
        else:
            print(f"  Missing {src}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Create compact KPI snapshots from ETL dashboard outputs.")
    parser.add_argument(
        "--etl-root",
        type=Path,
        default=Path(r"Z:\Vs - Code Work\ETL"),
    )
    parser.add_argument(
        "--destination",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "kpi_sources",
    )
    args = parser.parse_args()

    overview_source = args.etl_root / "output" / "overview"
    watch_html = args.etl_root / "output" / "watch_hours" / "veto_watch_hours.html"
    overview_html = overview_source / "overview_dashboard.html"
    overview_destination = args.destination / "overview"
    watch_destination = args.destination / "watch"

    if not overview_html.exists():
        raise FileNotFoundError(f"Overview dashboard not found: {overview_html}")
    if not watch_html.exists():
        raise FileNotFoundError(f"Watch-hours dashboard not found: {watch_html}")

    print(f"Reading ETL outputs from {args.etl_root}")
    copy_overview_files(overview_source, overview_destination)

    overview = embedded_json(overview_html, OVERVIEW_MARKER)
    write_json(
        overview_destination / "overview_snapshot.json",
        {
            "generated": overview.get("generated"),
            "report_date": overview.get("report_date"),
            "data_range": overview.get("data_range"),
            "rows": overview.get("rows", []),
            "device_summary": overview.get("device_summary", {}),
        },
    )
    print("  Wrote overview_snapshot.json")

    watch = embedded_json(watch_html, WATCH_MARKER)
    device_decode = watch.get("device_decode") or {}
    write_json(
        watch_destination / "watch_snapshot.json",
        {
            "generated_at": watch.get("generated_at"),
            "date_range": watch.get("date_range"),
            "true_data_range": watch.get("true_data_range"),
            "summary": watch.get("summary", {}),
            "daily": watch.get("daily", []),
            "channels": watch.get("channels", []),
            "channel_daily_all": watch.get("channel_daily_all", []),
            "geo": watch.get("geo", {}),
            "device": watch.get("device", []),
            "ua": watch.get("ua", []),
            "mapping_quality": watch.get("mapping_quality", []),
            "device_decode_ua_cache": device_decode.get("ua_cache", []),
        },
    )
    print("  Wrote watch_snapshot.json")


if __name__ == "__main__":
    main()
