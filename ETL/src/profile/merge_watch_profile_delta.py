#!/usr/bin/env python3
"""Merge a date-scoped watch-hours profile into the main materialized profile."""

from __future__ import annotations

import argparse
import os
from decimal import Decimal
from pathlib import Path

import pandas as pd


CHUNK_DURATION_HOURS = 6 / 3600.0

DAILY_TABLE_NAMES = [
    "daily_volume",
    "status_codes_daily",
    "extensions_daily",
    "hosts_daily",
    "geo_daily",
    "channel_geo_daily",
    "asn_daily",
    "cache_daily",
    "errors_daily",
    "query_params_daily",
    "query_param_keys_daily",
    "query_m_channel_daily",
    "channel_audience_daily",
    "region_channel_audience_daily",
    "cmcd_daily",
    "user_agents_daily",
    "device_type_by_channel_daily",
    "region_channel_device_daily",
    "mapping_quality_daily",
    "unmapped_candidates_daily",
]

PROFILE_DAILY_FILES = [
    "daily_volume",
    "channel_daily",
]

NUMERIC_COLUMN_EXACT = {
    "rows",
    "status_200_rows",
    "non_200_rows",
    "raw_ts_rows",
    "status_200_ts_rows",
    "ts_rows",
    "m3u8_rows",
    "raw_watch_hours",
    "status_200_watch_hours",
    "raw_ts_chunks",
    "status_200_ts_chunks",
    "approx_unique_ips",
    "approx_distinct_segments",
    "approx_distinct_paths",
    "approx_sessions",
    "approx_devices",
    "distinct_hosts",
    "size_bytes",
    "size_mb",
    "total_bytes",
    "response_content_len",
}
NUMERIC_SUFFIXES = (
    "_rows",
    "_chunks",
    "_hours",
    "_ips",
    "_hosts",
    "_bytes",
    "_mb",
    "_len",
    "_pct",
)
STRING_COLUMN_EXACT = {
    "statusCode",
    "errorCode",
    "startupError",
    "cacheStatus",
    "cacheable",
    "asn",
    "country",
    "state",
    "city",
    "extension",
    "reqHost",
    "candidate_id",
    "channel_name",
    "quality_bucket",
    "device_type",
    "userAgent",
    "UA",
    "sample_reqHost",
    "sample_reqPath",
    "sample_queryStr",
    "param_key",
    "sample_value",
    "m_value",
    "sample_cmcd",
    "source",
}


def _actual_path(base: Path) -> Path | None:
    parquet_path = base.with_suffix(".parquet")
    csv_path = base.with_suffix(".csv")
    if parquet_path.exists() and parquet_path.stat().st_size > 0:
        return parquet_path
    if csv_path.exists() and csv_path.stat().st_size > 0:
        return csv_path
    return None


def read_table(base: Path) -> pd.DataFrame:
    actual = _actual_path(base)
    if actual is None:
        return pd.DataFrame()
    if actual.suffix.lower() == ".parquet":
        return pd.read_parquet(actual)
    return pd.read_csv(actual)


def write_table(df: pd.DataFrame, base: Path) -> Path:
    out = base.with_suffix(".parquet")
    out.parent.mkdir(parents=True, exist_ok=True)
    df = normalize_for_parquet(df)
    tmp = out.with_name(f"tmp_{out.stem}_{os.getpid()}.parquet")
    df.to_parquet(tmp, index=False, compression="zstd")
    tmp.replace(out)
    print(f"wrote {out} ({len(df):,} rows)")
    return out


def should_be_numeric(column: str) -> bool:
    return column in NUMERIC_COLUMN_EXACT or column.endswith(NUMERIC_SUFFIXES)


def normalize_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "sample_queryStr" in out.columns:
        sample = out["sample_queryStr"].astype("string").fillna("")
        out["sample_queryStr"] = sample.where(sample.str.strip() == "", "[queryStr sample hidden]").astype(str)
    for column in out.columns:
        if column in STRING_COLUMN_EXACT:
            out[column] = out[column].astype("string").fillna("").astype(str)
            continue
        if should_be_numeric(column):
            out[column] = pd.to_numeric(out[column], errors="coerce").fillna(0)
            continue
        if out[column].dtype == "object":
            sample = out[column].dropna().head(100)
            if any(isinstance(value, Decimal) for value in sample):
                out[column] = pd.to_numeric(out[column], errors="coerce")
    return out


def normalize_log_date(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "log_date" not in df.columns:
        return df
    out = df.copy()
    out["log_date"] = pd.to_datetime(out["log_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return out.dropna(subset=["log_date"])


def numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    for column in columns:
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce").fillna(0)
    return out


def merge_date_rows(base: pd.DataFrame, delta: pd.DataFrame, dates: set[str]) -> pd.DataFrame:
    base = normalize_log_date(base)
    delta = normalize_log_date(delta)
    if "source" in delta.columns and "source" not in base.columns:
        base = base.copy()
        base["source"] = "stream"
    if "source" in base.columns and "source" not in delta.columns:
        delta = delta.copy()
        delta["source"] = "stream"
    if delta.empty:
        return base
    if "log_date" not in delta.columns:
        return delta

    delta_dates = set(delta["log_date"].astype(str).unique())
    replace_dates = dates or delta_dates
    delta = delta[delta["log_date"].astype(str).isin(replace_dates)].copy()
    if base.empty:
        merged = delta
    elif "log_date" in base.columns:
        kept = base[~base["log_date"].astype(str).isin(replace_dates)].copy()
        merged = pd.concat([kept, delta], ignore_index=True, sort=False)
    else:
        merged = delta

    sort_cols = [
        c for c in [
            "log_date",
            "source",
            "country",
            "state",
            "city",
            "channel_name",
            "device_type",
            "reqHost",
            "statusCode",
            "extension",
            "asn",
            "param_key",
            "m_value",
        ]
        if c in merged.columns
    ]
    if sort_cols:
        merged = merged.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)
    return merged


def merge_profile_tables(base_profile: Path, delta_profile: Path, dates: set[str]) -> None:
    for name in PROFILE_DAILY_FILES:
        merged = merge_date_rows(
            read_table(base_profile / f"{name}.csv"),
            read_table(delta_profile / f"{name}.csv"),
            dates,
        )
        if not merged.empty:
            write_table(merged, base_profile / f"{name}.csv")

    base_daily = base_profile.parent / "daily_tables"
    delta_daily = delta_profile.parent / "daily_tables"
    for name in DAILY_TABLE_NAMES:
        merged = merge_date_rows(
            read_table(base_daily / f"{name}.csv"),
            read_table(delta_daily / f"{name}.csv"),
            dates,
        )
        if not merged.empty:
            write_table(merged, base_daily / f"{name}.csv")


def group_frame(
    df: pd.DataFrame,
    keys: list[str],
    sum_cols: list[str],
    max_cols: list[str] | None = None,
    first_cols: list[str] | None = None,
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    present_keys = [c for c in keys if c in df.columns]
    if not present_keys:
        return pd.DataFrame()
    out = numeric(df, [c for c in sum_cols + (max_cols or []) if c in df.columns])
    aggs: dict[str, str] = {}
    for col in sum_cols:
        if col in out.columns and col not in present_keys:
            aggs[col] = "sum"
    for col in max_cols or []:
        if col in out.columns and col not in present_keys:
            aggs[col] = "max"
    for col in first_cols or []:
        if col in out.columns and col not in present_keys:
            aggs[col] = "first"
    if not aggs:
        return out[present_keys].drop_duplicates().reset_index(drop=True)
    grouped = out.groupby(present_keys, dropna=False).agg(aggs).reset_index()
    return grouped


def top(df: pd.DataFrame, sort_col: str, limit: int) -> pd.DataFrame:
    if df.empty or sort_col not in df.columns:
        return df
    return df.sort_values(sort_col, ascending=False).head(limit).reset_index(drop=True)


def rebuild_channel_summary(profile: Path) -> None:
    df = numeric(
        read_table(profile / "channel_daily.csv"),
        ["raw_ts_chunks", "raw_watch_hours", "status_200_ts_chunks", "status_200_watch_hours", "m3u8_rows", "approx_unique_ips"],
    )
    df = normalize_log_date(df)
    if df.empty or "channel_name" not in df.columns:
        return
    grouped = group_frame(
        df,
        ["channel_name"],
        ["raw_ts_chunks", "raw_watch_hours", "status_200_ts_chunks", "status_200_watch_hours", "m3u8_rows"],
        ["approx_unique_ips"],
    )
    first_last = df.groupby("channel_name", dropna=False)["log_date"].agg(first_seen="min", last_seen="max").reset_index()
    grouped = grouped.merge(first_last, on="channel_name", how="left")
    write_table(top(grouped, "raw_watch_hours", 10000), profile / "channel_summary.csv")


def rebuild_from_daily_tables(profile: Path, top_n: int) -> None:
    daily = profile.parent / "daily_tables"

    status = group_frame(
        read_table(daily / "status_codes_daily.csv"),
        ["statusCode"],
        ["rows", "raw_ts_rows", "status_200_ts_rows", "raw_watch_hours", "status_200_watch_hours"],
        ["approx_unique_ips"],
        ["sample_reqPath"],
    )
    if not status.empty:
        write_table(top(status, "rows", top_n), profile / "status_codes.csv")

    extensions = group_frame(
        read_table(daily / "extensions_daily.csv"),
        ["extension"],
        ["rows", "status_200_rows"],
        ["approx_unique_ips"],
        ["sample_reqPath"],
    )
    if not extensions.empty:
        extensions["non_200_rows"] = extensions["rows"] - extensions.get("status_200_rows", 0)
        write_table(top(extensions, "rows", top_n), profile / "extensions.csv")

    hosts = group_frame(
        read_table(daily / "hosts_daily.csv"),
        ["reqHost"],
        ["rows", "status_200_rows", "non_200_rows", "raw_ts_rows", "status_200_ts_rows", "raw_watch_hours", "status_200_watch_hours"],
        ["approx_unique_ips"],
    )
    if not hosts.empty:
        hosts["ts_rows"] = hosts.get("raw_ts_rows", 0)
        write_table(top(hosts, "rows", top_n), profile / "hosts_overview.csv")

    cache = group_frame(
        read_table(daily / "cache_daily.csv"),
        ["reqHost", "cacheStatus", "cacheable"],
        ["rows", "raw_ts_rows", "status_200_ts_rows"],
        ["approx_unique_ips"],
    )
    if not cache.empty:
        write_table(top(cache, "rows", top_n), profile / "cache_by_host.csv")

    errors = group_frame(
        read_table(daily / "errors_daily.csv"),
        ["reqHost", "statusCode", "errorCode", "startupError"],
        ["rows", "raw_ts_rows"],
        ["approx_unique_ips"],
        ["sample_reqPath"],
    )
    if not errors.empty:
        write_table(top(errors, "rows", top_n), profile / "errors_by_host.csv")

    geo = group_frame(
        read_table(daily / "geo_daily.csv"),
        ["country", "state", "city"],
        ["raw_ts_rows", "status_200_ts_rows", "raw_watch_hours", "status_200_watch_hours"],
        ["approx_unique_ips"],
    )
    if not geo.empty:
        geo["ts_rows"] = geo.get("raw_ts_rows", 0)
        write_table(top(geo, "ts_rows", top_n), profile / "geo_top.csv")

    asn = group_frame(
        read_table(daily / "asn_daily.csv"),
        ["asn"],
        ["raw_ts_rows", "status_200_ts_rows", "raw_watch_hours", "status_200_watch_hours"],
        ["approx_unique_ips", "distinct_hosts"],
        ["sample_reqHost"],
    )
    if not asn.empty:
        asn["ts_rows"] = asn.get("raw_ts_rows", 0)
        write_table(top(asn, "ts_rows", top_n), profile / "asn_top.csv")

    ua = group_frame(
        read_table(daily / "user_agents_daily.csv"),
        ["userAgent"],
        ["rows", "raw_ts_rows", "status_200_ts_rows"],
        ["approx_unique_ips", "distinct_hosts"],
    )
    if not ua.empty:
        ua = ua.rename(columns={"userAgent": "UA"})
        write_table(top(ua, "rows", top_n), profile / "ua_top.csv")

    device = group_frame(
        read_table(daily / "device_type_by_channel_daily.csv"),
        ["channel_name", "device_type"],
        ["raw_ts_rows", "status_200_ts_rows"],
        ["approx_unique_ips"],
    )
    if not device.empty:
        device["ts_rows"] = device.get("raw_ts_rows", 0)
        write_table(top(device, "ts_rows", top_n), profile / "device_type_by_channel.csv")

    mapping = group_frame(
        read_table(daily / "mapping_quality_daily.csv"),
        ["reqHost", "candidate_id", "channel_name", "quality_bucket"],
        ["rows", "raw_ts_chunks", "status_200_ts_chunks", "raw_watch_hours", "status_200_watch_hours"],
        ["approx_unique_ips"],
        ["sample_reqPath"],
    )
    if not mapping.empty:
        write_table(top(mapping, "raw_watch_hours", top_n), profile / "path_candidate_quality.csv")

    unmapped = group_frame(
        read_table(daily / "unmapped_candidates_daily.csv"),
        ["reqHost", "candidate_id"],
        ["rows", "raw_ts_chunks", "status_200_ts_chunks", "raw_watch_hours", "status_200_watch_hours"],
        ["approx_unique_ips"],
        ["sample_reqPath"],
    )
    if not unmapped.empty:
        write_table(top(unmapped, "raw_watch_hours", top_n), profile / "unmapped_candidates.csv")

    query = read_table(daily / "query_params_daily.csv")
    if not query.empty:
        num_cols = [c for c in query.columns if c not in {"log_date", "source", "sample_queryStr"}]
        row = numeric(query, num_cols)[num_cols].sum().to_frame().T
        if "sample_queryStr" in query.columns:
            row["sample_queryStr"] = query["sample_queryStr"].dropna().astype(str).head(1).tolist()[0] if query["sample_queryStr"].notna().any() else ""
        write_table(row, profile / "querystr_param_presence.csv")

    cmcd = read_table(daily / "cmcd_daily.csv")
    if not cmcd.empty:
        num_cols = [c for c in cmcd.columns if c not in {"log_date", "source", "sample_cmcd"}]
        row = numeric(cmcd, num_cols)[num_cols].sum().to_frame().T
        if "sample_cmcd" in cmcd.columns:
            row["sample_cmcd"] = cmcd["sample_cmcd"].dropna().astype(str).head(1).tolist()[0] if cmcd["sample_cmcd"].notna().any() else ""
        write_table(row, profile / "cmcd_presence.csv")

    rebuild_channel_summary(profile)


def rebuild_file_inventory(lake: Path | None, profile: Path) -> None:
    if lake is None or not lake.exists():
        return
    rows = []
    for file in lake.glob("**/*.parquet"):
        parts = {}
        for piece in file.parts:
            if "=" in piece:
                key, value = piece.split("=", 1)
                parts[key] = value
        date_text = ""
        if {"year", "month", "day"} <= set(parts):
            date_text = f"{int(parts['year']):04d}-{int(parts['month']):02d}-{int(parts['day']):02d}"
        rows.append(
            {
                "date": date_text,
                "source": parts.get("source", ""),
                "file": str(file),
                "size_bytes": file.stat().st_size,
                "size_mb": round(file.stat().st_size / 1024 / 1024, 3),
            }
        )
    if rows:
        write_table(pd.DataFrame(rows).sort_values(["date", "source", "file"]), profile / "file_inventory.csv")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge a date-scoped watch-hours profile into the main profile.")
    parser.add_argument("--base-profile", type=Path, required=True)
    parser.add_argument("--delta-profile", type=Path, required=True)
    parser.add_argument("--dates", nargs="+", default=[], help="IST dates to replace, YYYY-MM-DD.")
    parser.add_argument("--lake", type=Path, default=None, help="Optional lake folder for refreshing file_inventory.")
    parser.add_argument("--top-n", type=int, default=1000)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_profile = args.base_profile.resolve()
    delta_profile = args.delta_profile.resolve()
    if not delta_profile.exists():
        raise SystemExit(f"Delta profile not found: {delta_profile}")

    dates = set(args.dates)
    base_profile.mkdir(parents=True, exist_ok=True)
    (base_profile.parent / "daily_tables").mkdir(parents=True, exist_ok=True)

    print(f"Base profile : {base_profile}")
    print(f"Delta profile: {delta_profile}")
    print(f"Replace dates: {', '.join(sorted(dates)) if dates else 'delta dates'}")

    merge_profile_tables(base_profile, delta_profile, dates)
    rebuild_from_daily_tables(base_profile, int(args.top_n))
    rebuild_file_inventory(args.lake.resolve() if args.lake else None, base_profile)
    print("Profile delta merge complete.")


if __name__ == "__main__":
    main()
