#!/usr/bin/env python3
"""
generate_matrix_report.py
═══════════════════════════════════════════════════════════════════════
Production-grade Excel report generator for Hive-partitioned Parquet lakes.
All dates displayed in IST (UTC+5:30) while partition filter remains UTC.
Uses shared utilities from shared_utils.py.
"""

from __future__ import annotations

import logging
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import psutil
import duckdb
import pandas as pd
import pyarrow.parquet as pq
import xlsxwriter
import xlsxwriter.utility

# Import shared utilities
from shared_utils import (
    get_conn, lake_reader, qs_extract, channel_clean_expr,
    IST_OFFSET, utc_date_to_ist_str, utc_date_str_to_ist_str,
)

# ═══════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="  %(levelname)-7s %(message)s",
)
log = logging.getLogger("matrix_report")

# ═══════════════════════════════════════════════════════════════
# CONFIG DATACLASS (specific to this report)
# ═══════════════════════════════════════════════════════════════

@dataclass
class MatrixConfig:
    lake_root:    Path
    output_path:  Path
    year:         Optional[str] = None
    month:        Optional[str] = None          # zero-padded "05"
    matrix_top_n: int           = 40_000        # max distinct values per matrix sheet
    matrix_days:  int           = 60            # how many recent days to show as columns
    threads:      int           = 2
    memory_gb:    int           = 8
    temp_dir:     Optional[Path] = None

    col_ua:       str = "UA"
    col_asn:      str = "asn"
    col_city:     str = "city"
    col_state:    str = "state"
    col_country:  str = "country"
    col_reqhost:  str = "reqHost"
    col_qs:       str = "queryStr"
    col_ip:       str = "cliIP"
    col_ts:       str = "reqTimeSec"

    _partition_cols: frozenset = field(default_factory=lambda: frozenset({"year", "month", "day"}))


# ═══════════════════════════════════════════════════════════════
# AUTO-SCALE CONFIG
# ═══════════════════════════════════════════════════════════════

def auto_config(lake_root: Path) -> MatrixConfig:
    mem   = psutil.virtual_memory()
    total = mem.total  / (1024 ** 3)
    free  = mem.available / (1024 ** 3)
    cpus  = os.cpu_count() or 4

    mem_gb = max(4, min(int(free * 0.70), 48))
    threads = max(1, min(cpus // 4, 4))

    if total < 16:
        top_n, days = 50_000, 30
    elif total < 32:
        top_n, days = 50_000, 45
    else:
        top_n, days = 50_000, 60

    EXCEL_MAX_DATA_ROWS = 1_048_576 - 5
    top_n = min(top_n, EXCEL_MAX_DATA_ROWS)

    log.info(f"System: {cpus} cores | {total:.1f} GiB total | {free:.1f} GiB free")
    log.info(f"DuckDB: {threads} threads | {mem_gb} GiB limit")
    log.info(f"Matrix: top {top_n:,} values × {days} days")

    return MatrixConfig(
        lake_root=lake_root,
        output_path=lake_root.parent / "matrix_report.xlsx",
        threads=threads,
        memory_gb=mem_gb,
        matrix_top_n=top_n,
        matrix_days=days,
    )


# ═══════════════════════════════════════════════════════════════
# INTERACTIVE PROMPTS (unchanged)
# ═══════════════════════════════════════════════════════════════

def prompt_lake_folder() -> Path:
    while True:
        raw = input("\n📁 Lake folder path: ").strip()
        if not raw:
            print("  Path cannot be empty.")
            continue
        path = Path(raw).resolve()
        if not path.exists():
            print(f"  ❌ Not found: {path}")
            continue
        year_dirs = [d for d in path.iterdir() if d.is_dir() and d.name.startswith("year=")]
        if not year_dirs:
            print(f"  ⚠️  No year= partitions found. Continue? (y/n): ", end="")
            if input().strip().lower() != "y":
                continue
        log.info(f"Lake folder: {path}")
        return path

def prompt_output_path(default: Path) -> Path:
    print(f"\n💾 Output path (Enter = {default}): ", end="")
    raw = input().strip()
    out = Path(raw).resolve() if raw else default
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.suffix.lower() != ".xlsx":
        out = out.with_suffix(".xlsx")
    log.info(f"Output: {out}")
    return out

def prompt_month_filter(lake_root: Path) -> tuple[Optional[str], Optional[str]]:
    print("\n📅 Filter by month? (y/n): ", end="")
    try:
        if input().strip().lower() != "y":
            return None, None
        available_years = sorted(
            d.name.replace("year=", "")
            for d in lake_root.iterdir()
            if d.is_dir() and d.name.startswith("year=")
        )
        if available_years:
            print(f"  Available years in lake: {', '.join(available_years)}")
        while True:
            year_in = input("  Year (e.g. 2026): ").strip()
            if not year_in.isdigit():
                print(f"  ❌ Not a number")
                continue
            y = int(year_in)
            if not (2000 <= y <= 2100):
                print("  ❌ Year out of range")
                continue
            if available_years and year_in not in available_years:
                print(f"  ⚠️  year={year_in} not found. Continue? (y/n): ", end="")
                if input().strip().lower() != "y":
                    continue
            break
        while True:
            month_in = input("  Month 1–12 (e.g. 5): ").strip()
            if not month_in.isdigit():
                print(f"  ❌ Not a number")
                continue
            m = int(month_in)
            if not (1 <= m <= 12):
                print("  ❌ Month must be 1–12")
                continue
            break
        year_str  = str(y)
        month_str = f"{m:02d}"
        partition = lake_root / f"year={year_str}" / f"month={month_str}"
        if not partition.exists():
            print(f"  ⚠️  Partition not found: {partition}. Continue? (y/n): ", end="")
            if input().strip().lower() != "y":
                return None, None
        log.info(f"Month filter: year={year_str}, month={month_str}")
        return year_str, month_str
    except KeyboardInterrupt:
        print("\n  Cancelled.")
        return None, None

def prompt_confirm(cfg: MatrixConfig) -> bool:
    print("\n" + "═" * 58)
    print("  Confirm settings:")
    print(f"    Lake   : {cfg.lake_root}")
    print(f"    Output : {cfg.output_path}")
    if cfg.year:
        print(f"    Filter : year={cfg.year}, month={cfg.month}")
    else:
        print(f"    Filter : none — full lake")
    print(f"    Matrix : top {cfg.matrix_top_n:,} values × {cfg.matrix_days} days")
    print("═" * 58)
    while True:
        print("\n  Proceed? (y/n): ", end="")
        ans = input().strip().lower()
        if ans == "y":
            return True
        if ans == "n":
            return False
        print("  Please type y or n")


# ═══════════════════════════════════════════════════════════════
# PYARROW HELPERS (still needed for metadata, no conflict)
# ═══════════════════════════════════════════════════════════════

def pa_schema(lake_root: Path, year: Optional[str] = None, month: Optional[str] = None) -> dict[str, str]:
    scan_root = lake_root
    if year:
        scan_root = lake_root / f"year={year}"
        if month:
            scan_root = scan_root / f"month={month}"
    schema_map: dict[str, str] = {}
    _partition_skip = {"year", "month", "day"}
    for parquet_file in scan_root.rglob("*.parquet"):
        try:
            s = pq.read_schema(parquet_file)
            for name in s.names:
                if name in _partition_skip:
                    continue
                if name not in schema_map:
                    schema_map[name] = str(s.field(name).type)
        except Exception as exc:
            log.warning(f"Could not read schema from {parquet_file.name}: {exc}")
        if len(schema_map) > 5:
            break
    return schema_map

def pa_row_counts_by_day(
    lake_root: Path,
    year: Optional[str] = None,
    month: Optional[str] = None,
) -> dict[str, int]:
    scan_root = lake_root
    if year:
        scan_root = lake_root / f"year={year}"
        if month:
            scan_root = scan_root / f"month={month}"
    day_counts: dict[str, int] = {}
    for day_dir in sorted(scan_root.rglob("day=*")):
        if not day_dir.is_dir():
            continue
        try:
            parts   = {p.split("=")[0]: p.split("=")[1] for p in day_dir.parts if "=" in p}
            y, mo   = parts.get("year", "0"), parts.get("month", "0")
            d       = parts.get("day", "0")
            date_key = f"{y}-{mo}-{d}"
            total   = 0
            for pf in day_dir.glob("*.parquet"):
                try:
                    meta  = pq.read_metadata(pf)
                    total += meta.num_rows
                except Exception:
                    pass
            day_counts[date_key] = day_counts.get(date_key, 0) + total
        except Exception as exc:
            log.warning(f"Row count skip {day_dir}: {exc}")
    return day_counts

def pa_total_file_count(lake_root: Path) -> int:
    return sum(1 for _ in lake_root.rglob("*.parquet"))


# ═══════════════════════════════════════════════════════════════
# XLSXWRITER STYLES (still needed here)
# ═══════════════════════════════════════════════════════════════

class Styles:
    def __init__(self, wb: xlsxwriter.Workbook):
        def _f(**kw) -> xlsxwriter.format.Format:
            base = {
                "font_name": "Arial", "font_size": 10,
                "border": 1, "border_color": "#CCCCCC",
                "valign": "vcenter",
            }
            base.update(kw)
            return wb.add_format(base)
        self.title  = _f(font_size=11, bold=True,
                         font_color="#FFFFFF", bg_color="#1F3864", align="left")
        self.hdr    = _f(bold=True, font_color="#FFFFFF", bg_color="#2E75B6",
                         align="center", text_wrap=True)
        self.kv_key = _f(bold=True, font_color="#000000", bg_color="#D6E4F0", align="left")
        self.kv_val = _f(font_color="#000000", bg_color="#FFFFFF", align="left")
        self.data   = _f(font_color="#000000", bg_color="#FFFFFF", align="left")
        self.alt    = _f(font_color="#000000", bg_color="#F2F2F2", align="left")
        self.num    = _f(font_color="#000000", bg_color="#FFFFFF", align="right", num_format="#,##0")
        self.num_alt= _f(font_color="#000000", bg_color="#F2F2F2", align="right", num_format="#,##0")
        self.pct    = _f(font_color="#000000", bg_color="#FFFFFF", align="right", num_format="0.00%")
        self.pct_alt= _f(font_color="#000000", bg_color="#F2F2F2", align="right", num_format="0.00%")
        self.total  = _f(bold=True, font_color="#FFFFFF", bg_color="#E36209", align="right", num_format="#,##0")
        self.total_l= _f(bold=True, font_color="#FFFFFF", bg_color="#E36209", align="left")

def title_row(ws, row: int, text: str, n_cols: int, s: Styles) -> None:
    ws.write(row, 0, text, s.title)
    for c in range(1, n_cols):
        ws.write(row, c, "", s.title)

def kv(ws, row: int, key: str, val, s: Styles) -> None:
    ws.write(row, 0, key, s.kv_key)
    ws.write(row, 1, str(val), s.kv_val)


# ═══════════════════════════════════════════════════════════════
# SHEET 1 — OVERVIEW (uses IST helpers from shared_utils)
# ═══════════════════════════════════════════════════════════════

def build_overview(
    wb:    xlsxwriter.Workbook,
    con:   duckdb.DuckDBPyConnection,
    cfg:   MatrixConfig,
    cols:  set[str],
    s:     Styles,
) -> None:
    log.info("Building Overview …")
    ws = wb.add_worksheet("Overview")
    ws.set_column(0, 0, 30)
    ws.set_column(1, 1, 38)
    for c in range(2, 15):
        ws.set_column(c, c, 16)

    row = 0
    title_row(ws, row, "📊  Report Metadata", 2, s); row += 1

    ist_now = datetime.now(timezone.utc) + IST_OFFSET
    kv(ws, row, "Report generated", ist_now.strftime("%Y-%m-%d %H:%M IST"), s); row += 1

    if cfg.year:
        kv(ws, row, "Filtered to", f"year={cfg.year}, month={cfg.month}", s); row += 1
    kv(ws, row, "Lake folder", str(cfg.lake_root), s); row += 1
    kv(ws, row, "DuckDB version", duckdb.__version__, s); row += 1

    log.info("  Reading row counts from Parquet metadata (zero-scan) …")
    day_counts = pa_row_counts_by_day(cfg.lake_root, cfg.year, cfg.month)
    total_rows_pa = sum(day_counts.values())
    total_files   = pa_total_file_count(cfg.lake_root / (f"year={cfg.year}" if cfg.year else "") if cfg.year else cfg.lake_root)
    kv(ws, row, "Total rows (metadata)", f"{total_rows_pa:,}", s); row += 1
    kv(ws, row, "Total parquet files", f"{total_files:,}", s); row += 1

    ts = cfg.col_ts
    if ts in cols:
        expr = lake_reader(cfg.lake_root, cfg.year, cfg.month)
        meta = safe_query(con, f"""
            SELECT
                MIN(to_timestamp(CAST({ts} AS DOUBLE)))::DATE AS min_date,
                MAX(to_timestamp(CAST({ts} AS DOUBLE)))::DATE AS max_date,
                MIN(to_timestamp(CAST({ts} AS DOUBLE)))       AS min_ts,
                MAX(to_timestamp(CAST({ts} AS DOUBLE)))       AS max_ts
            FROM {expr}
        """, "date range")
        if not meta.empty:
            r = meta.iloc[0]
            min_ist_date = r['min_date'] + IST_OFFSET
            max_ist_date = r['max_date'] + IST_OFFSET
            min_ist_ts   = r['min_ts'] + IST_OFFSET
            max_ist_ts   = r['max_ts'] + IST_OFFSET
            kv(ws, row, "Data date range (IST)",
               f"{min_ist_date.strftime('%Y-%m-%d')} → {max_ist_date.strftime('%Y-%m-%d')}", s); row += 1
            kv(ws, row, "Data time range (IST)",
               f"{min_ist_ts.strftime('%Y-%m-%d %H:%M:%S')} → {max_ist_ts.strftime('%Y-%m-%d %H:%M:%S')}", s); row += 1
    row += 1

    N_COLS = 15
    title_row(ws, row, "📅  Day-by-Day Breakdown (IST dates)", N_COLS, s); row += 1
    hdr_labels = [
        "Date (IST)", "Total Rows*", "cliIP rows", "Distinct cliIP",
        "Distinct cliIP & UA", "Total Bytes (GiB)",
        "Rows w/ session_id = 1", "Distinct session_id", "session_id = Null", "session_id = NA",
        "Rows w/ device_id = 1", "Distinct device_id", "device_id = Null", "device_id = NA",
    ]
    for c, h in enumerate(hdr_labels):
        ws.write(row, c, h, s.hdr)
    row += 1

    if ts in cols:
        qs      = cfg.col_qs
        ip_col  = cfg.col_ip
        ua_col  = cfg.col_ua
        has_qs  = qs in cols
        has_ip  = ip_col in cols
        has_ua  = ua_col in cols

        ip_rows     = f"COUNT({ip_col})" if has_ip else "0"
        d_ip        = f"COUNT(DISTINCT {ip_col})" if has_ip else "0"
        distinct_ipua = f"COUNT(DISTINCT ({ip_col}, {ua_col}))" if (has_ip and has_ua) else "NULL"

        total_bytes_col = "totalBytes" if "totalBytes" in cols else None
        sum_bytes = f"SUM(CAST({total_bytes_col} AS BIGINT))" if total_bytes_col else "NULL"

        if has_qs:
            sess_present  = f"SUM(CASE WHEN {qs} LIKE '%session_id=%' THEN 1 ELSE 0 END)"
            dev_present   = f"SUM(CASE WHEN {qs} LIKE '%device_id=%' THEN 1 ELSE 0 END)"
            sess_distinct = f"COUNT(DISTINCT {qs_extract(qs, 'session_id')})"
            dev_distinct  = f"COUNT(DISTINCT {qs_extract(qs, 'device_id')})"
            dev_blank     = (f"SUM(CASE WHEN {qs} LIKE '%device_id=%' AND {qs_extract(qs,'device_id')} IS NULL THEN 1 ELSE 0 END)")
            sess_blank    = (f"SUM(CASE WHEN {qs} LIKE '%session_id=%' AND {qs_extract(qs,'session_id')} IS NULL THEN 1 ELSE 0 END)")
            dev_missing   = f"SUM(CASE WHEN {qs} IS NULL OR {qs} NOT LIKE '%device_id=%' THEN 1 ELSE 0 END)"
            sess_missing  = f"SUM(CASE WHEN {qs} IS NULL OR {qs} NOT LIKE '%session_id=%' THEN 1 ELSE 0 END)"
            dev_nonblank  = f"({dev_present} - {dev_blank})"
            sess_nonblank = f"({sess_present} - {sess_blank})"
        else:
            sess_present = sess_distinct = dev_present = dev_distinct = "0"
            dev_blank = sess_blank = dev_missing = sess_missing = "0"
            dev_nonblank = sess_nonblank = "0"

        expr = lake_reader(cfg.lake_root, cfg.year, cfg.month)
        day_df = safe_query(con, f"""
            SELECT
                make_date(CAST(year AS INT), CAST(month AS INT), CAST(day AS INT)) AS utc_date,
                COUNT(*)         AS total_rows,
                {ip_rows}        AS ip_rows,
                {d_ip}           AS distinct_ip,
                {distinct_ipua}  AS distinct_ipua,
                {sum_bytes}      AS total_bytes,
                {sess_nonblank}  AS sess_nonblank,
                {sess_distinct}  AS distinct_sess,
                {sess_blank}     AS sess_blank,
                {sess_missing}   AS sess_missing,
                {dev_nonblank}   AS dev_nonblank,
                {dev_distinct}   AS distinct_dev,
                {dev_blank}      AS dev_blank,
                {dev_missing}    AS dev_missing
            FROM {expr}
            GROUP BY year, month, day
            ORDER BY utc_date
        """, "day breakdown")

        data_start = row
        for _, r in day_df.iterrows():
            utc_dt = r["utc_date"]
            ist_date_str = utc_date_to_ist_str(utc_dt)
            is_alt = (row - data_start) % 2 == 1
            pa_count = day_counts.get(str(utc_dt), int(r["total_rows"]))
            bytes_val = r["total_bytes"] if total_bytes_col and pd.notna(r["total_bytes"]) else 0
            gb_val = bytes_val / (1024 ** 3) if bytes_val else 0

            vals = [
                ist_date_str, pa_count,
                int(r["ip_rows"]), int(r["distinct_ip"]),
                int(r["distinct_ipua"]) if (has_ip and has_ua) else 0,
                round(gb_val, 2),
                int(r["sess_nonblank"]), int(r["distinct_sess"]),
                int(r["sess_blank"]), int(r["sess_missing"]),
                int(r["dev_nonblank"]), int(r["distinct_dev"]),
                int(r["dev_blank"]), int(r["dev_missing"]),
            ]
            for c, v in enumerate(vals):
                fmt = (s.alt if is_alt else s.data) if c == 0 else (s.num_alt if is_alt else s.num)
                ws.write(row, c, v, fmt)
            row += 1

        if len(day_df):
            data_end = row
            ws.write(row, 0, "TOTAL*", s.total_l)
            for c in range(1, N_COLS):
                col_l = xlsxwriter.utility.xl_col_to_name(c)
                ws.write(row, c, f"=SUM({col_l}{data_start + 1}:{col_l}{data_end})", s.total)
            row += 1

    ws.write(row + 1, 0,
             "* Total Rows sourced from Parquet file metadata (zero-scan). "
             "All other columns via DuckDB aggregation.", s.data)
    ws.freeze_panes(2, 1)


# ═══════════════════════════════════════════════════════════════
# SHEET 2 — CHANNEL × PLATFORM (reuses channel_clean_expr from shared)
# ═══════════════════════════════════════════════════════════════

def build_channel_platform(
    wb:   xlsxwriter.Workbook,
    con:  duckdb.DuckDBPyConnection,
    cfg:  MatrixConfig,
    cols: set[str],
    s:    Styles,
) -> None:
    log.info("Building Channel×Platform …")
    ws = wb.add_worksheet("Channel×Platform")
    for c, w in enumerate([40, 20, 16, 16, 16, 16]):
        ws.set_column(c, c, w)

    qs = cfg.col_qs
    N  = 6
    title_row(ws, 0, "📡  Channel × Platform — requests, devices, sessions", N, s)
    if qs not in cols:
        ws.write(1, 0, "queryStr column not found.", s.data)
        return
    for c, h in enumerate(["Channel", "Platform", "Requests", "Unique Devices", "Unique Sessions", "% of Requests"]):
        ws.write(1, c, h, s.hdr)

    expr = lake_reader(cfg.lake_root, cfg.year, cfg.month)
    df = safe_query(con, f"""
        SELECT
            {channel_clean_expr(qs)}                              AS channel,
            COALESCE({qs_extract(qs, 'platform')}, 'Unknown')     AS platform,
            COUNT(*)                                              AS requests,
            COUNT(DISTINCT {qs_extract(qs, 'device_id')})         AS devices,
            COUNT(DISTINCT {qs_extract(qs, 'session_id')})        AS sessions
        FROM {expr}
        WHERE {qs} IS NOT NULL AND {qs} LIKE '%channel=%'
        GROUP BY 1, 2
        ORDER BY 3 DESC
        LIMIT 5000
    """, "channel×platform")

    if df.empty:
        ws.write(2, 0, "No channel data found.", s.data)
        return
    total_req = int(df["requests"].sum())
    for i, r in enumerate(df.itertuples(index=False)):
        is_alt = i % 2 == 1
        d, n   = (s.alt, s.num_alt) if is_alt else (s.data, s.num)
        pct    = r.requests / total_req if total_req else 0
        ws.write(i + 2, 0, r.channel,  d)
        ws.write(i + 2, 1, r.platform, d)
        ws.write(i + 2, 2, int(r.requests), n)
        ws.write(i + 2, 3, int(r.devices),  n)
        ws.write(i + 2, 4, int(r.sessions), n)
        ws.write(i + 2, 5, round(pct, 4),   s.pct_alt if is_alt else s.pct)
    ws.freeze_panes(2, 0)


# ═══════════════════════════════════════════════════════════════
# MATRIX SHEET (uses IST helpers from shared_utils)
# ═══════════════════════════════════════════════════════════════

def safe_query(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    label: str = "",
    params: list | None = None,
) -> pd.DataFrame:
    try:
        return con.execute(sql, params or []).df()
    except Exception as exc:
        log.warning(f"Query failed [{label}]: {exc}")
        return pd.DataFrame()

def _discover_day_partitions(
    lake_root: Path,
    year:     Optional[str],
    month:    Optional[str],
    max_days: int,
) -> list[tuple[str, Path]]:
    days: list[tuple[str, Path]] = []
    if year and month:
        month_dir = lake_root / f"year={year}" / f"month={month}"
        if not month_dir.exists():
            return []
        for child in month_dir.iterdir():
            if not child.is_dir() or not child.name.startswith("day="):
                continue
            d = child.name[4:]
            days.append((f"{year}-{month}-{d}", child))
        days.sort(key=lambda x: x[0], reverse=True)
        return days[:max_days]
    elif year:
        year_dir = lake_root / f"year={year}"
        if not year_dir.exists():
            return []
        for month_dir in sorted(year_dir.iterdir(), reverse=True):
            if not month_dir.is_dir() or not month_dir.name.startswith("month="):
                continue
            mo = month_dir.name[6:]
            for day_dir in sorted(month_dir.iterdir(), reverse=True):
                if not day_dir.is_dir() or not day_dir.name.startswith("day="):
                    continue
                d = day_dir.name[4:]
                days.append((f"{year}-{mo}-{d}", day_dir))
                if len(days) >= max_days:
                    return days
        return days[:max_days]
    else:
        for year_dir in sorted(lake_root.iterdir(), reverse=True):
            if not year_dir.is_dir() or not year_dir.name.startswith("year="):
                continue
            y = year_dir.name[5:]
            for month_dir in sorted(year_dir.iterdir(), reverse=True):
                if not month_dir.is_dir() or not month_dir.name.startswith("month="):
                    continue
                mo = month_dir.name[6:]
                for day_dir in sorted(month_dir.iterdir(), reverse=True):
                    if not day_dir.is_dir() or not day_dir.name.startswith("day="):
                        continue
                    d = day_dir.name[4:]
                    days.append((f"{y}-{mo}-{d}", day_dir))
                    if len(days) >= max_days:
                        return days
        return days[:max_days]

def _matrix_counts_by_day(
    con:       duckdb.DuckDBPyConnection,
    day_parts: list[tuple[str, Path]],
    col_expr:  str,
    top_vals:  set[str],
    label:     str,
) -> dict[str, dict[str, int]]:
    results: dict[str, dict[str, int]] = {}
    for i, (date_str, day_dir) in enumerate(day_parts):
        pattern = f"{day_dir.as_posix()}/*.parquet"
        if not list(day_dir.glob("*.parquet")):
            log.warning(f"  No parquet files in {day_dir} – skipping {date_str}")
            continue
        reader = f"read_parquet('{pattern}', union_by_name=true)"
        df = safe_query(con, f"""
            SELECT
                COALESCE(CAST({col_expr} AS VARCHAR), '(null)') AS value,
                COUNT(*) AS cnt
            FROM {reader}
            WHERE {col_expr} IS NOT NULL
              AND TRIM(CAST({col_expr} AS VARCHAR)) <> ''
            GROUP BY 1
        """, f"{label}[{date_str}]")
        if df.empty:
            continue
        day_dict = {str(r.value): int(r.cnt) for r in df.itertuples(index=False) if str(r.value) in top_vals}
        if day_dict:
            results[date_str] = day_dict
        if (i + 1) % 10 == 0:
            log.info(f"    {i + 1}/{len(day_parts)} days processed …")
    return results

def build_matrix(
    wb:        xlsxwriter.Workbook,
    con:       duckdb.DuckDBPyConnection,
    cfg:       MatrixConfig,
    cols:      set[str],
    s:         Styles,
    col_expr:  str,
    label:     str,
) -> None:
    sheet_name = re.sub(r"[:\[\]*?/\\]", "_", label)[:31] + "_Matrix"
    log.info(f"Building matrix: {label} …")
    ws = wb.add_worksheet(sheet_name)
    ws.set_column(0, 0, 52)

    ts = cfg.col_ts
    if ts not in cols:
        ws.write(0, 0, f"reqTimeSec missing — cannot build matrix for {label}", s.data)
        return

    day_parts = _discover_day_partitions(cfg.lake_root, cfg.year, cfg.month, cfg.matrix_days)
    if not day_parts:
        ws.write(0, 0, f"No day partitions found.", s.data)
        return
    log.info(f"  {len(day_parts)} day partitions found (last {cfg.matrix_days} days)")

    EXCEL_MAX_DATA_ROWS = 1_048_576 - 5
    safe_top_n = min(cfg.matrix_top_n, EXCEL_MAX_DATA_ROWS)
    log.info(f"  Fetching top {safe_top_n:,} values …")
    expr = lake_reader(cfg.lake_root, cfg.year, cfg.month)
    top_df = safe_query(con, f"""
        SELECT
            COALESCE(CAST({col_expr} AS VARCHAR), '(null)') AS value,
            COUNT(*) AS total
        FROM {expr}
        WHERE {col_expr} IS NOT NULL
          AND TRIM(CAST({col_expr} AS VARCHAR)) <> ''
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT {safe_top_n + 1}
    """, f"top_{label}")

    if top_df.empty:
        ws.write(0, 0, f"No data for {label}.", s.data)
        return

    truncated = False
    if len(top_df) > safe_top_n:
        truncated = True
        top_df = top_df.iloc[:safe_top_n]
    n_vals = len(top_df)
    top_vals = set(top_df["value"].tolist())
    log.info(f"  {n_vals:,} distinct values found" + (" (truncated to Excel row limit)" if truncated else ""))

    log.info(f"  Computing daily counts (day-by-day, OOM-safe) …")
    matrix_data = _matrix_counts_by_day(con, day_parts, col_expr, top_vals, label)
    if not matrix_data:
        ws.write(0, 0, f"No daily data for {label}.", s.data)
        return

    log.info(f"  Pivoting …")
    utc_dates = sorted(matrix_data.keys(), reverse=True)
    n_dates = len(utc_dates)
    row_order = list(top_df["value"])
    for c in range(1, n_dates + 1):
        ws.set_column(c, c, 12)

    row = 0
    title_row(ws, row, f"📅  {label} Matrix — top {n_vals:,} values × last {n_dates} days (IST dates)", n_dates + 1, s)
    row += 1

    ist_headers = [label] + [utc_date_str_to_ist_str(d) for d in utc_dates]
    for c, h in enumerate(ist_headers):
        ws.write(row, c, h, s.hdr)
    row += 1

    data_start = row
    log.info(f"  Writing {n_vals:,} rows …")
    for idx, val in enumerate(row_order):
        is_alt = idx % 2 == 1
        d, n   = (s.alt, s.num_alt) if is_alt else (s.data, s.num)
        ws.write(row, 0, val, d)
        for c, utc_date in enumerate(utc_dates, 1):
            v = matrix_data.get(utc_date, {}).get(val, 0)
            ws.write(row, c, v, n)
        row += 1
        if (idx + 1) % 10_000 == 0:
            log.info(f"    {idx + 1:,} rows written …")

    ws.write(row, 0, "TOTAL", s.total_l)
    for c in range(1, n_dates + 1):
        col_l = xlsxwriter.utility.xl_col_to_name(c)
        ws.write(row, c, f"=SUM({col_l}{data_start + 1}:{col_l}{row})", s.total)
    row += 1

    if truncated:
        warning_msg = f"⚠️  Only top {safe_top_n:,} values shown (Excel row limit). Values omitted."
        ws.write(row, 0, warning_msg, s.data)
        warning_format = wb.add_format({'bg_color': '#FFCCCC', 'font_color': '#990000', 'bold': True})
        for c in range(n_dates + 1):
            ws.write(row, c, warning_msg if c == 0 else "", warning_format)
        row += 1

    ws.freeze_panes(2, 1)
    log.info(f"  Matrix '{label}' done ✓")


# ═══════════════════════════════════════════════════════════════
# NEW: IP MATRIX WITH STATE & CHANNEL
# ═══════════════════════════════════════════════════════════════

def build_ip_matrix_with_state_channel(
    wb:   xlsxwriter.Workbook,
    con:  duckdb.DuckDBPyConnection,
    cfg:  MatrixConfig,
    cols: set[str],
    s:    Styles,
) -> None:
    """Client IP matrix with extra columns: State (mode) and Channel (mode)."""
    ip_col = cfg.col_ip
    state_col = cfg.col_state
    qs_col = cfg.col_qs

    # Check required columns
    if ip_col not in cols:
        log.warning("Client IP matrix skipped: 'cliIP' column missing.")
        return

    sheet_name = "Client IP_Matrix"
    log.info(f"Building {sheet_name} with extra columns (State, Channel) …")
    ws = wb.add_worksheet(sheet_name)

    # Set column widths: IP, State, Channel, then day columns
    ws.set_column(0, 0, 45)   # IP
    ws.set_column(1, 1, 20)   # State
    ws.set_column(2, 2, 30)   # Channel

    # Discover day partitions (same as other matrices)
    if cfg.col_ts not in cols:
        ws.write(0, 0, "reqTimeSec missing — cannot build matrix", s.data)
        return
    day_parts = _discover_day_partitions(cfg.lake_root, cfg.year, cfg.month, cfg.matrix_days)
    if not day_parts:
        ws.write(0, 0, "No day partitions found.", s.data)
        return
    log.info(f"  {len(day_parts)} day partitions found (last {cfg.matrix_days} days)")

    # Top N IPs
    EXCEL_MAX_DATA_ROWS = 1_048_576 - 5
    safe_top_n = min(cfg.matrix_top_n, EXCEL_MAX_DATA_ROWS)
    log.info(f"  Fetching top {safe_top_n:,} IPs …")
    expr = lake_reader(cfg.lake_root, cfg.year, cfg.month)
    top_ips_df = safe_query(con, f"""
        SELECT
            COALESCE(CAST({ip_col} AS VARCHAR), '(null)') AS ip,
            COUNT(*) AS total
        FROM {expr}
        WHERE {ip_col} IS NOT NULL
          AND TRIM(CAST({ip_col} AS VARCHAR)) <> ''
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT {safe_top_n + 1}
    """, "top_client_ips")

    if top_ips_df.empty:
        ws.write(0, 0, "No IP data found.", s.data)
        return

    truncated = False
    if len(top_ips_df) > safe_top_n:
        truncated = True
        top_ips_df = top_ips_df.iloc[:safe_top_n]
    top_ips = set(top_ips_df["ip"].tolist())
    n_ips = len(top_ips)
    log.info(f"  {n_ips:,} distinct IPs" + (" (truncated to Excel row limit)" if truncated else ""))

    # For each IP, get mode state and mode channel (overall, across all selected dates)
    # We'll compute this in one query: group by IP, state, channel, count; then rank within IP.
    # Use a subquery to get top-1 state and top-1 channel per IP based on frequency.
    # However, state and channel may not always be present; we'll left join later.
    # Simpler: two separate queries for mode state and mode channel, then combine.
    log.info("  Computing most frequent state and channel for each IP …")
    mode_state_df = pd.DataFrame()
    mode_channel_df = pd.DataFrame()

    if state_col in cols:
        mode_state_df = safe_query(con, f"""
            WITH ip_state_counts AS (
                SELECT
                    COALESCE(CAST({ip_col} AS VARCHAR), '(null)') AS ip,
                    COALESCE(CAST({state_col} AS VARCHAR), 'Unknown') AS state,
                    COUNT(*) AS cnt
                FROM {expr}
                WHERE {ip_col} IS NOT NULL
                  AND TRIM(CAST({ip_col} AS VARCHAR)) <> ''
                GROUP BY ip, state
            ),
            ranked AS (
                SELECT ip, state,
                       ROW_NUMBER() OVER (PARTITION BY ip ORDER BY cnt DESC) AS rn
                FROM ip_state_counts
            )
            SELECT ip, state
            FROM ranked
            WHERE rn = 1
        """, "mode_state_per_ip")
        log.info(f"    State mode computed for {len(mode_state_df)} IPs")

    if qs_col in cols:
        # Channel cleaning expression
        channel_expr = channel_clean_expr(qs_col)
        mode_channel_df = safe_query(con, f"""
            WITH ip_channel_counts AS (
                SELECT
                    COALESCE(CAST({ip_col} AS VARCHAR), '(null)') AS ip,
                    COALESCE({channel_expr}, 'Unknown') AS channel,
                    COUNT(*) AS cnt
                FROM {expr}
                WHERE {ip_col} IS NOT NULL
                  AND TRIM(CAST({ip_col} AS VARCHAR)) <> ''
                  AND {qs_col} IS NOT NULL AND {qs_col} LIKE '%channel=%'
                GROUP BY ip, channel
            ),
            ranked AS (
                SELECT ip, channel,
                       ROW_NUMBER() OVER (PARTITION BY ip ORDER BY cnt DESC) AS rn
                FROM ip_channel_counts
            )
            SELECT ip, channel
            FROM ranked
            WHERE rn = 1
        """, "mode_channel_per_ip")
        log.info(f"    Channel mode computed for {len(mode_channel_df)} IPs")

    # Create a mapping for state and channel per IP
    ip_state = dict(zip(mode_state_df["ip"], mode_state_df["state"])) if not mode_state_df.empty else {}
    ip_channel = dict(zip(mode_channel_df["ip"], mode_channel_df["channel"])) if not mode_channel_df.empty else {}

    # Daily counts per IP (only for top IPs)
    log.info("  Computing daily counts per IP (day-by-day) …")
    matrix_data = _matrix_counts_by_day(con, day_parts, ip_col, top_ips, "client_ip")
    if not matrix_data:
        ws.write(0, 0, "No daily data for IPs.", s.data)
        return

    utc_dates = sorted(matrix_data.keys(), reverse=True)
    n_dates = len(utc_dates)
    for c in range(3, 3 + n_dates):
        ws.set_column(c, c, 12)

    # Write headers: IP, State, Channel, then dates (IST)
    row = 0
    title_row(ws, row, f"📅  Client IP Matrix — top {n_ips:,} IPs × last {n_dates} days (IST dates)", 3 + n_dates, s)
    row += 1
    headers = ["Client IP", "State", "Channel"] + [utc_date_str_to_ist_str(d) for d in utc_dates]
    for c, h in enumerate(headers):
        ws.write(row, c, h, s.hdr)
    row += 1

    data_start = row
    log.info(f"  Writing {n_ips:,} rows …")
    for idx, ip in enumerate(top_ips_df["ip"]):
        is_alt = idx % 2 == 1
        d, n = (s.alt, s.num_alt) if is_alt else (s.data, s.num)
        state = ip_state.get(ip, "N/A")
        channel = ip_channel.get(ip, "N/A")
        ws.write(row, 0, ip, d)
        ws.write(row, 1, state, d)
        ws.write(row, 2, channel, d)
        for c, utc_date in enumerate(utc_dates, 3):
            cnt = matrix_data.get(utc_date, {}).get(ip, 0)
            ws.write(row, c, cnt, n)
        row += 1
        if (idx + 1) % 10_000 == 0:
            log.info(f"    {idx + 1:,} rows written …")

    # Total row
    ws.write(row, 0, "TOTAL", s.total_l)
    ws.write(row, 1, "", s.total_l)
    ws.write(row, 2, "", s.total_l)
    for c in range(3, 3 + n_dates):
        col_l = xlsxwriter.utility.xl_col_to_name(c)
        ws.write(row, c, f"=SUM({col_l}{data_start + 1}:{col_l}{row})", s.total)
    row += 1

    if truncated:
        warning_msg = f"⚠️  Only top {safe_top_n:,} IPs shown (Excel row limit). Values omitted."
        ws.write(row, 0, warning_msg, s.data)
        warning_format = wb.add_format({'bg_color': '#FFCCCC', 'font_color': '#990000', 'bold': True})
        for c in range(3 + n_dates):
            ws.write(row, c, warning_msg if c == 0 else "", warning_format)
        row += 1

    ws.freeze_panes(2, 3)  # Freeze after IP, State, Channel columns
    log.info(f"  Client IP matrix (with State & Channel) done ✓")


# ═══════════════════════════════════════════════════════════════
# NEW: TOP 200 CHANNELS PER STATE
# ═══════════════════════════════════════════════════════════════

def build_top_channels_by_state(
    wb:   xlsxwriter.Workbook,
    con:  duckdb.DuckDBPyConnection,
    cfg:  MatrixConfig,
    cols: set[str],
    s:    Styles,
) -> None:
    """Create a sheet showing top 200 channels for each state."""
    state_col = cfg.col_state
    qs_col = cfg.col_qs

    if state_col not in cols or qs_col not in cols:
        log.warning("Top Channels by State skipped: missing 'state' or 'queryStr' column.")
        return

    log.info("Building Top Channels by State …")
    ws = wb.add_worksheet("Top Channels by State")
    ws.set_column(0, 0, 20)   # State
    ws.set_column(1, 1, 40)   # Channel
    ws.set_column(2, 2, 16)   # Requests
    ws.set_column(3, 3, 10)   # Rank

    expr = lake_reader(cfg.lake_root, cfg.year, cfg.month)
    channel_expr = channel_clean_expr(qs_col)

    # Get top 200 channels per state, ordered by total requests
    # Use row_number() over (partition by state order by requests desc)
    query = f"""
        WITH state_channel_totals AS (
            SELECT
                COALESCE(CAST({state_col} AS VARCHAR), 'Unknown') AS state,
                COALESCE({channel_expr}, 'Unknown') AS channel,
                COUNT(*) AS requests
            FROM {expr}
            WHERE {state_col} IS NOT NULL
              AND TRIM(CAST({state_col} AS VARCHAR)) <> ''
              AND {qs_col} IS NOT NULL AND {qs_col} LIKE '%channel=%'
            GROUP BY state, channel
        ),
        ranked AS (
            SELECT state, channel, requests,
                   ROW_NUMBER() OVER (PARTITION BY state ORDER BY requests DESC) AS rank
            FROM state_channel_totals
        )
        SELECT state, channel, requests, rank
        FROM ranked
        WHERE rank <= 200
        ORDER BY state, rank
    """
    df = safe_query(con, query, "top_channels_by_state")
    if df.empty:
        ws.write(0, 0, "No data found.", s.data)
        return

    # Write header
    headers = ["State", "Channel", "Total Requests", "Rank"]
    for c, h in enumerate(headers):
        ws.write(0, c, h, s.hdr)

    # Write data
    current_state = None
    row = 1
    for _, r in df.iterrows():
        is_alt = (row % 2 == 0)
        d = s.alt if is_alt else s.data
        n = s.num_alt if is_alt else s.num
        ws.write(row, 0, r["state"], d)
        ws.write(row, 1, r["channel"], d)
        ws.write(row, 2, int(r["requests"]), n)
        ws.write(row, 3, int(r["rank"]), n)
        row += 1

    ws.freeze_panes(1, 0)
    log.info(f"  Top Channels by State done ✓ (rows: {row - 1})")

# ═══════════════════════════════════════════════════════════════
# NEW: DEVICE MATRIX WITH STATE (plus %20 cleaning)
# ═══════════════════════════════════════════════════════════════

def build_device_matrix_with_state(
    wb:   xlsxwriter.Workbook,
    con:  duckdb.DuckDBPyConnection,
    cfg:  MatrixConfig,
    cols: set[str],
    s:    Styles,
) -> None:
    """Device matrix with an extra column: most frequent State per Device.
       Also cleans %20 from Device and State strings."""
    device_col_expr = qs_extract(cfg.col_qs, 'device')
    state_col = cfg.col_state
    qs_col = cfg.col_qs

    # Check required columns
    if cfg.col_qs not in cols:
        log.warning("Device matrix skipped: queryStr column missing.")
        return
    if state_col not in cols:
        log.warning("Device matrix without State: 'state' column missing.")
        # Fall back to generic matrix? We'll just exit.
        return

    sheet_name = "Device_Matrix"
    log.info(f"Building {sheet_name} with extra State column …")
    ws = wb.add_worksheet(sheet_name)

    # Column widths
    ws.set_column(0, 0, 40)   # Device
    ws.set_column(1, 1, 20)   # State
    for c in range(2, 2 + cfg.matrix_days):
        ws.set_column(c, c, 12)

    # Discover day partitions
    if cfg.col_ts not in cols:
        ws.write(0, 0, "reqTimeSec missing — cannot build matrix", s.data)
        return
    day_parts = _discover_day_partitions(cfg.lake_root, cfg.year, cfg.month, cfg.matrix_days)
    if not day_parts:
        ws.write(0, 0, "No day partitions found.", s.data)
        return
    log.info(f"  {len(day_parts)} day partitions found (last {cfg.matrix_days} days)")

    # Top N devices (overall)
    EXCEL_MAX_DATA_ROWS = 1_048_576 - 5
    safe_top_n = min(cfg.matrix_top_n, EXCEL_MAX_DATA_ROWS)
    log.info(f"  Fetching top {safe_top_n:,} devices …")
    expr = lake_reader(cfg.lake_root, cfg.year, cfg.month)
    top_devices_df = safe_query(con, f"""
        SELECT
            COALESCE(CAST({device_col_expr} AS VARCHAR), '(null)') AS device,
            COUNT(*) AS total
        FROM {expr}
        WHERE {qs_col} IS NOT NULL
          AND {device_col_expr} IS NOT NULL
          AND TRIM(CAST({device_col_expr} AS VARCHAR)) <> ''
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT {safe_top_n + 1}
    """, "top_devices")

    if top_devices_df.empty:
        ws.write(0, 0, "No device data found.", s.data)
        return

    truncated = False
    if len(top_devices_df) > safe_top_n:
        truncated = True
        top_devices_df = top_devices_df.iloc[:safe_top_n]
    top_devices = set(top_devices_df["device"].tolist())
    n_devices = len(top_devices)
    log.info(f"  {n_devices:,} distinct devices" + (" (truncated to Excel row limit)" if truncated else ""))

    # Compute most frequent state per device (overall)
    log.info("  Computing most frequent state for each device …")
    mode_state_df = safe_query(con, f"""
        WITH device_state_counts AS (
            SELECT
                COALESCE(CAST({device_col_expr} AS VARCHAR), '(null)') AS device,
                COALESCE(CAST({state_col} AS VARCHAR), 'Unknown') AS state,
                COUNT(*) AS cnt
            FROM {expr}
            WHERE {qs_col} IS NOT NULL
              AND {device_col_expr} IS NOT NULL
              AND TRIM(CAST({device_col_expr} AS VARCHAR)) <> ''
              AND {state_col} IS NOT NULL
              AND TRIM(CAST({state_col} AS VARCHAR)) <> ''
            GROUP BY device, state
        ),
        ranked AS (
            SELECT device, state,
                   ROW_NUMBER() OVER (PARTITION BY device ORDER BY cnt DESC) AS rn
            FROM device_state_counts
        )
        SELECT device, state
        FROM ranked
        WHERE rn = 1
    """, "mode_state_per_device")
    device_state_map = dict(zip(mode_state_df["device"], mode_state_df["state"])) if not mode_state_df.empty else {}

    # Daily counts per device (only for top devices)
    log.info("  Computing daily counts per device …")
    # We need a version of _matrix_counts_by_day that works with an expression (device_col_expr)
    # Reuse the existing one, but it expects a simple column expression. device_col_expr is complex (function call).
    # We'll pass the raw expression string; _matrix_counts_by_day will use it in SQL.
    # However, _matrix_counts_by_day uses "CAST({col_expr} AS VARCHAR)" – that works with any expression.
    matrix_data = _matrix_counts_by_day(con, day_parts, device_col_expr, top_devices, "device")
    if not matrix_data:
        ws.write(0, 0, "No daily data for devices.", s.data)
        return

    utc_dates = sorted(matrix_data.keys(), reverse=True)
    n_dates = len(utc_dates)

    # Write headers: Device, State, then dates (IST)
    row = 0
    title_row(ws, row, f"📅  Device Matrix — top {n_devices:,} devices × last {n_dates} days (IST dates)", 2 + n_dates, s)
    row += 1
    headers = ["Device", "State"] + [utc_date_str_to_ist_str(d) for d in utc_dates]
    for c, h in enumerate(headers):
        ws.write(row, c, h, s.hdr)
    row += 1

    data_start = row
    log.info(f"  Writing {n_devices:,} rows …")
    for idx, device in enumerate(top_devices_df["device"]):
        is_alt = idx % 2 == 1
        d, n = (s.alt, s.num_alt) if is_alt else (s.data, s.num)

        # Clean %20 from device and state
        device_clean = device.replace("%20", " ")
        state_clean = device_state_map.get(device, "N/A").replace("%20", " ")

        ws.write(row, 0, device_clean, d)
        ws.write(row, 1, state_clean, d)
        for c, utc_date in enumerate(utc_dates, 2):
            cnt = matrix_data.get(utc_date, {}).get(device, 0)
            ws.write(row, c, cnt, n)
        row += 1
        if (idx + 1) % 10_000 == 0:
            log.info(f"    {idx + 1:,} rows written …")

    # Total row
    ws.write(row, 0, "TOTAL", s.total_l)
    ws.write(row, 1, "", s.total_l)
    for c in range(2, 2 + n_dates):
        col_l = xlsxwriter.utility.xl_col_to_name(c)
        ws.write(row, c, f"=SUM({col_l}{data_start + 1}:{col_l}{row})", s.total)
    row += 1

    if truncated:
        warning_msg = f"⚠️  Only top {safe_top_n:,} devices shown (Excel row limit). Values omitted."
        ws.write(row, 0, warning_msg, s.data)
        warning_format = wb.add_format({'bg_color': '#FFCCCC', 'font_color': '#990000', 'bold': True})
        for c in range(2 + n_dates):
            ws.write(row, c, warning_msg if c == 0 else "", warning_format)
        row += 1

    ws.freeze_panes(2, 2)  # Freeze after Device and State columns
    log.info(f"  Device matrix (with State) done ✓")

# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main() -> None:
    print("\n" + "═" * 58)
    print("  Matrix Report Generator  (IST dates, PyArrow + DuckDB + XlsxWriter)")
    print("═" * 58 + "\n")

    if len(sys.argv) > 1:
        lake_root = Path(sys.argv[1]).resolve()
        if not lake_root.exists():
            log.error(f"Folder not found: {lake_root}")
            sys.exit(1)
    else:
        lake_root = prompt_lake_folder()

    cfg = auto_config(lake_root)

    cfg.output_path      = prompt_output_path(cfg.output_path)
    cfg.year, cfg.month  = prompt_month_filter(lake_root)

    if not prompt_confirm(cfg):
        print("  Cancelled.")
        sys.exit(0)

    log.info("Detecting schema via PyArrow (reading Parquet footers) …")
    schema_map = pa_schema(lake_root, cfg.year, cfg.month)
    cols: set[str] = set(schema_map.keys())
    log.info(f"  {len(cols)} columns: {', '.join(sorted(cols))}")

    log.info("Connecting to DuckDB …")
    con = get_conn()
    con.execute(f"SET threads={cfg.threads};")
    con.execute(f"SET memory_limit='{cfg.memory_gb}GB';")
    con.execute("SET preserve_insertion_order=false;")
    if cfg.temp_dir:
        td = cfg.temp_dir
    else:
        td = Path(os.environ.get("TEMP", os.environ.get("TMP", "/tmp"))) / "duckdb_matrix_report"
    td.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory='{td.as_posix()}';")
    log.info(f"DuckDB temp/spill dir: {td}")

    log.info("Creating Excel workbook (constant_memory=True) …")
    wb = xlsxwriter.Workbook(str(cfg.output_path), {"constant_memory": True, "strings_to_urls": False})
    s = Styles(wb)

    build_overview(wb, con, cfg, cols, s)
    build_channel_platform(wb, con, cfg, cols, s)

    # === MODIFIED: Use specialised IP matrix with state & channel ===
    build_ip_matrix_with_state_channel(wb, con, cfg, cols, s)

    # === NEW: Top 200 channels per state ===
    build_top_channels_by_state(wb, con, cfg, cols, s)

    # Other matrices remain unchanged
    direct_fields = [
        (cfg.col_ua,      "User Agent"),
        (cfg.col_asn,     "ASN"),
        (cfg.col_city,    "City"),
        (cfg.col_state,   "State"),
        (cfg.col_country, "Country"),
        (cfg.col_reqhost, "Request Host"),
        # (cfg.col_ip,      "Client IP"),   # replaced by specialised function
    ]
    for col_name, label in direct_fields:
        if col_name in cols:
            build_matrix(wb, con, cfg, cols, s, col_name, label)
        else:
            log.info(f"Skipping {label} matrix — column '{col_name}' not in lake")

    qs = cfg.col_qs
    if qs in cols:
        # Special handling for Device matrix (adds State column)
        build_device_matrix_with_state(wb, con, cfg, cols, s)

        # Other QS matrices use generic builder
        qs_fields = [
            (channel_clean_expr(qs),              "Channel"),
            (qs_extract(qs, "platform"),          "Platform"),
            # (qs_extract(qs, "device"),          "Device"),   # replaced by specialised
            (qs_extract(qs, "category_name"),     "Category"),
            (qs_extract(qs, "content_title"),     "Content Title"),
        ]
        for col_expr, label in qs_fields:
            build_matrix(wb, con, cfg, cols, s, col_expr, label)
    else:
        log.info(f"Skipping queryString matrices — column '{qs}' not in lake")

    log.info(f"Saving → {cfg.output_path}")
    wb.close()
    con.close()

    print(f"\n{'═' * 58}")
    print(f"  ✅  Report saved: {cfg.output_path}")
    print(f"{'═' * 58}\n")

if __name__ == "__main__":
    main()