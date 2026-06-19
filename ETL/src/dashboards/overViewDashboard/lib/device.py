"""
lib/device.py — Device snapshot and daily CSV processing.
"""

import csv
from pathlib import Path
from .extract import coerce_float


def read_device_data(snapshot_path: Path, daily_path: Path) -> tuple[list, list, dict]:
    """
    Returns (snapshot_rows, daily_rows, first_seen_counts).
    Both CSV paths are optional; missing files are silently skipped.
    """
    snapshot_rows: list[dict] = []
    device_idx: dict[str, int] = {}
    first_seen_counts: dict[str, int] = {}
    daily_map: dict[str, dict] = {}
    first_seen_by_device: dict[int, str] = {}
    first_seen_by_source_device: dict[tuple[str, int], str] = {}

    # ── Snapshot CSV ─────────────────────────────────────────────────────────
    if snapshot_path.exists():
        with snapshot_path.open("r", encoding="utf-8", newline="") as f:
            for r in csv.DictReader(f):
                device_id = r.get("device_id", "")
                if not device_id:
                    continue
                device_idx[device_id] = len(device_idx)
                first_seen = r.get("first_seen_utc_date", "")
                if first_seen:
                    first_seen_counts[first_seen] = first_seen_counts.get(first_seen, 0) + 1
                snapshot_rows.append({
                    "device_id":  device_id,
                    "first_seen": first_seen,
                    "last_seen":  r.get("last_seen_utc_date", ""),
                    "days_seen":  int(coerce_float(r.get("days_seen"))),
                    "total_rows": int(coerce_float(r.get("total_rows"))),
                    "sessions":   int(coerce_float(r.get("distinct_sessions_day_sum"))),
                })

    # ── Daily CSV ─────────────────────────────────────────────────────────────
    if daily_path.exists():
        with daily_path.open("r", encoding="utf-8", newline="") as f:
            for r in csv.DictReader(f):
                d = r.get("utc_date", "")
                device_id = r.get("device_id", "")
                if not d or not device_id:
                    continue
                source = (r.get("source") or "stream").strip().lower() or "stream"
                idx = device_idx.get(device_id)
                if idx is None:
                    idx = len(device_idx)
                    device_idx[device_id] = idx
                old_first = first_seen_by_device.get(idx)
                if old_first is None or d < old_first:
                    first_seen_by_device[idx] = d
                source_key = (source, idx)
                old_source_first = first_seen_by_source_device.get(source_key)
                if old_source_first is None or d < old_source_first:
                    first_seen_by_source_device[source_key] = d
                item = daily_map.setdefault(
                    d, {"date": d, "devices": set(), "rows": 0, "sessions": 0, "sources": {}}
                )
                item["devices"].add(idx)
                item["rows"] += int(coerce_float(r.get("rows_on_date")))
                item["sessions"] += int(coerce_float(r.get("distinct_sessions")))
                source_item = item["sources"].setdefault(
                    source, {"devices": set(), "rows": 0, "sessions": 0}
                )
                source_item["devices"].add(idx)
                source_item["rows"] += int(coerce_float(r.get("rows_on_date")))
                source_item["sessions"] += int(coerce_float(r.get("distinct_sessions")))

    daily_rows = [
        {
            "date":           k,
            "devices":        sorted(v["devices"]),
            "active_devices": len(v["devices"]),
            "new_devices":    sum(1 for idx in v["devices"] if first_seen_by_device.get(idx) == k),
            "rows":           v["rows"],
            "sessions":       v["sessions"],
            "sources": {
                source: {
                    "devices": sorted(src_v["devices"]),
                    "active_devices": len(src_v["devices"]),
                    "new_devices": sum(
                        1
                        for idx in src_v["devices"]
                        if first_seen_by_source_device.get((source, idx)) == k
                    ),
                    "rows": src_v["rows"],
                    "sessions": src_v["sessions"],
                }
                for source, src_v in sorted(v["sources"].items())
            },
        }
        for k, v in sorted(daily_map.items())
    ]

    return snapshot_rows, daily_rows, first_seen_counts


def build_device_summary(snapshot_rows: list, daily_rows: list) -> dict:
    """Derive headline device metrics from processed snapshot/daily data."""
    summary = {
        "total_devices": 0,
        "latest_date": "",
        "active_latest": 0,
        "new_latest": 0,
        "returning_latest": 0,
    }
    if snapshot_rows:
        summary["total_devices"] = len(snapshot_rows)
    if daily_rows:
        latest = max(daily_rows, key=lambda d: d["date"])
        summary["latest_date"] = latest["date"]
        summary["active_latest"] = int(latest["active_devices"])
        summary["new_latest"] = sum(
            1 for d in snapshot_rows if d["first_seen"] == latest["date"]
        )
        summary["returning_latest"] = max(
            0, summary["active_latest"] - summary["new_latest"]
        )
    return summary
