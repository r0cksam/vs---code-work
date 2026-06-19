"""Shared source-aware data range helpers for dashboard headers."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd


IST_OFFSET_SECONDS = 19_800


def format_epoch_ist(epoch: float | int | None) -> str:
    """Format a UTC epoch second value as an IST timestamp string."""
    if epoch is None:
        return ""
    try:
        return datetime.fromtimestamp(float(epoch) + IST_OFFSET_SECONDS, timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    except (TypeError, ValueError, OSError):
        return ""


def range_from_datetime_strings(values: Iterable[str]) -> dict[str, str]:
    """Return first/last from existing YYYY-MM-DD HH:MM:SS-like strings."""
    clean = sorted(str(value).strip() for value in values if str(value).strip())
    if not clean:
        return {"first": "", "last": ""}
    return {"first": clean[0], "last": clean[-1]}


def source_ranges_from_daily(
    df: pd.DataFrame,
    *,
    source_col: str = "source",
    date_col: str = "log_date",
) -> list[dict[str, str]]:
    """Build date-only source ranges from an aggregate daily table."""
    if df.empty or source_col not in df.columns or date_col not in df.columns:
        return []
    out: list[dict[str, str]] = []
    for source, group in df.groupby(df[source_col].fillna("").astype(str).str.lower()):
        dates = sorted(str(value)[:10] for value in group[date_col].dropna().tolist() if str(value).strip())
        if not source or not dates:
            continue
        out.append(
            {
                "source": source,
                "min_date": dates[0],
                "max_date": dates[-1],
                "first": f"{dates[0]} 00:00:00",
                "last": f"{dates[-1]} 23:59:59",
                "first_ist": f"{dates[0]} 00:00:00",
                "last_ist": f"{dates[-1]} 23:59:59",
                "basis": "daily aggregate date range",
            }
        )
    return out


def combined_range(source_ranges: list[dict[str, str]]) -> dict[str, str]:
    """Return a combined first/last range across source range records."""
    first_values = [
        str(row.get("first_ist") or row.get("first") or "").strip()
        for row in source_ranges
        if str(row.get("first_ist") or row.get("first") or "").strip()
    ]
    last_values = [
        str(row.get("last_ist") or row.get("last") or "").strip()
        for row in source_ranges
        if str(row.get("last_ist") or row.get("last") or "").strip()
    ]
    if not first_values or not last_values:
        return {"first": "", "last": ""}
    return {"first": min(first_values), "last": max(last_values)}


def _min_max_req_epoch(files: list[Path]) -> tuple[float | None, float | None]:
    if not files:
        return None, None
    try:
        import pyarrow.parquet as pq
    except Exception:
        return None, None

    low: float | None = None
    high: float | None = None
    for file in files:
        try:
            parquet = pq.ParquetFile(file)
            if "reqTimeSec" not in parquet.schema_arrow.names:
                continue
            for batch in parquet.iter_batches(columns=["reqTimeSec"], batch_size=1_000_000):
                values = pd.to_numeric(batch.column(0).to_pandas(), errors="coerce").dropna()
                if values.empty:
                    continue
                batch_min = float(values.min())
                batch_max = float(values.max())
                low = batch_min if low is None else min(low, batch_min)
                high = batch_max if high is None else max(high, batch_max)
        except Exception:
            continue
    return low, high


def lake_partition_files(lake_root: Path, source: str, date_text: str) -> list[Path]:
    try:
        year, month, day = date_text.split("-", 2)
    except ValueError:
        return []
    root = lake_root / f"source={source}" / f"year={int(year):04d}" / f"month={int(month):02d}" / f"day={int(day):02d}"
    if not root.exists():
        return []
    return sorted(root.glob("*.parquet"))


def true_source_ranges_from_lake(
    source_dates: dict[str, Iterable[str]],
    lake_root: Path,
) -> list[dict[str, str]]:
    """Return source ranges using reqTimeSec min/max from first and last source partitions."""
    rows: list[dict[str, str]] = []
    for raw_source, raw_dates in sorted(source_dates.items()):
        source = str(raw_source).lower().strip()
        dates = sorted(str(date)[:10] for date in raw_dates if str(date).strip())
        if not source or not dates:
            continue
        first_date = dates[0]
        last_date = dates[-1]
        first_min, _ = _min_max_req_epoch(lake_partition_files(lake_root, source, first_date))
        _, last_max = _min_max_req_epoch(lake_partition_files(lake_root, source, last_date))
        first_ist = format_epoch_ist(first_min) or f"{first_date} 00:00:00"
        last_ist = format_epoch_ist(last_max) or f"{last_date} 23:59:59"
        rows.append(
            {
                "source": source,
                "min_date": first_date,
                "max_date": last_date,
                "first": first_ist,
                "last": last_ist,
                "first_ist": first_ist,
                "last_ist": last_ist,
                "basis": "reqTimeSec min/max from first and last lake partitions",
            }
        )
    return rows
