#!/usr/bin/env python3
"""
lake_report_ist_final.py
═══════════════════════════════════════════════════════════════════════
Production-grade Excel report generator for Hive-partitioned Parquet lakes.
All dates displayed in IST (UTC+5:30) while partition filter remains UTC.
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

# ═══════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="  %(levelname)-7s %(message)s",
)
log = logging.getLogger("lake_report")

# IST offset
IST_OFFSET = timedelta(hours=5, minutes=30)

def utc_date_to_ist_str(utc_date) -> str:
    """Convert a UTC date (datetime.date or datetime) to IST date string dd/mm/yy."""
    if isinstance(utc_date, pd.Timestamp):
        dt = utc_date.to_pydatetime()
    elif isinstance(utc_date, datetime):
        dt = utc_date
    else:
        dt = datetime.combine(utc_date, datetime.min.time())
    # Assume dt is UTC. Add IST offset.
    ist_dt = dt + IST_OFFSET
    return ist_dt.strftime("%d/%m/%y")

def utc_date_str_to_ist_str(utc_str: str) -> str:
    """Convert UTC date string '2026-05-01' to IST date string '01/05/26'."""
    y, m, d = map(int, utc_str.split('-'))
    dt = datetime(y, m, d, tzinfo=timezone.utc)
    ist_dt = dt + IST_OFFSET
    return ist_dt.strftime("%d/%m/%y")

# ═══════════════════════════════════════════════════════════════
# CONFIG DATACLASS
# ═══════════════════════════════════════════════════════════════

@dataclass
class ReportConfig:
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

def auto_config(lake_root: Path) -> ReportConfig:
    mem   = psutil.virtual_memory()
    total = mem.total  / (1024 ** 3)
    free  = mem.available / (1024 ** 3)
    cpus  = os.cpu_count() or 4

    mem_gb = max(4, min(int(free * 0.70), 48))
    threads = max(1, min(cpus // 4, 4))

    if total < 16:
        top_n, days = 15_000, 30
    elif total < 32:
        top_n, days = 30_000, 45
    else:
        top_n, days = 40_000, 60

    EXCEL_MAX_DATA_ROWS = 1_048_576 - 5
    top_n = min(top_n, EXCEL_MAX_DATA_ROWS)

    log.info(f"System: {cpus} cores | {total:.1f} GiB total | {free:.1f} GiB free")
    log.info(f"DuckDB: {threads} threads | {mem_gb} GiB limit")
    log.info(f"Matrix: top {top_n:,} values × {days} days")

    return ReportConfig(
        lake_root=lake_root,
        output_path=lake_root.parent / "lake_report.xlsx",
        threads=threads,
        memory_gb=mem_gb,
        matrix_top_n=top_n,
        matrix_days=days,
    )


# ═══════════════════════════════════════════════════════════════
# INTERACTIVE PROMPTS
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

def prompt_confirm(cfg: ReportConfig) -> bool:
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
# PYARROW HELPERS
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
# DUCKDB HELPERS
# ═══════════════════════════════════════════════════════════════

def duck_conn(cfg: ReportConfig) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET threads={cfg.threads};")
    con.execute(f"SET memory_limit='{cfg.memory_gb}GB';")
    con.execute("SET preserve_insertion_order=false;")
    if cfg.temp_dir:
        td = cfg.temp_dir
    else:
        td = Path(os.environ.get("TEMP", os.environ.get("TMP", "/tmp"))) / "duckdb_lake_report"
    td.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory='{td.as_posix()}';")
    log.info(f"DuckDB temp/spill dir: {td}")
    return con

def lake_expr(cfg: ReportConfig) -> str:
    if cfg.year and cfg.month:
        partition_path = cfg.lake_root / f"year={cfg.year}" / f"month={cfg.month}"
        if partition_path.exists():
            return (
                f"read_parquet('{partition_path.as_posix()}/**/*.parquet', "
                f"union_by_name=true)"
            )
        log.warning(f"Partition path missing: {partition_path} — falling back to full lake")
    return (
        f"read_parquet('{cfg.lake_root.as_posix()}/**/*.parquet', "
        f"hive_partitioning=true, union_by_name=true)"
    )

def qs_extract(qs_col: str, param: str) -> str:
    return f"NULLIF(regexp_extract({qs_col}, '(?:^|&){param}=([^&]+)', 1), '')"

_PLATFORM_SUFFIX_PATTERN = (
    "_(firetv|firestick|fireos|androidtv|android|webos|web|"
    "ios|iphone|ipad|appletv|apple|samsung|samsungtv|tizen|"
    "roku|mi|mitv|xiaomi|sony|bravia|lg|lgtv|"
    "mobile|phone|tablet|tv)$"
)

def channel_clean_expr(qs_col: str) -> str:
    raw = (
        f"COALESCE("
        f"  {qs_extract(qs_col, 'channel')},"
        f"  {qs_extract(qs_col, 'channel_name')},"
        f"  'Unknown'"
        f")"
    )
    return (
        f"TRIM(regexp_replace("
        f"  url_decode({raw}),"
        f"  '{_PLATFORM_SUFFIX_PATTERN}',"
        f"  '', 'i'"
        f"))"
    )

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


# ═══════════════════════════════════════════════════════════════
# XLSXWRITER STYLES
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
# SHEET 1 — OVERVIEW (all IST)
# ═══════════════════════════════════════════════════════════════

def build_overview(
    wb:    xlsxwriter.Workbook,
    con:   duckdb.DuckDBPyConnection,
    cfg:   ReportConfig,
    cols:  set[str],
    s:     Styles,
    ts:    str,
) -> None:
    log.info("Building Overview …")
    ws = wb.add_worksheet("Overview")
    ws.set_column(0, 0, 30)
    ws.set_column(1, 1, 38)
    for c in range(2, 15):
        ws.set_column(c, c, 16)

    row = 0
    title_row(ws, row, "📊  Report Metadata", 2, s); row += 1

    # Report generated in IST
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

    # Date/time range in IST
    if ts in cols:
        expr = lake_expr(cfg)
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
            # Convert UTC to IST
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

        expr = lake_expr(cfg)
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
# SHEET 2 — DISTINCT COUNTS (optional)
# ═══════════════════════════════════════════════════════════════

def build_distinct_counts(
    wb:   xlsxwriter.Workbook,
    con:  duckdb.DuckDBPyConnection,
    cfg:  ReportConfig,
    cols: set[str],
    s:    Styles,
) -> None:
    log.info("Building Distinct Counts …")
    ws = wb.add_worksheet("Distinct Counts")
    ws.set_column(0, 0, 28)
    ws.set_column(1, 1, 18)
    ws.set_column(2, 2, 20)
    ws.set_column(3, 3, 22)

    expr = lake_expr(cfg)
    qs   = cfg.col_qs
    selects: list[str] = []
    field_meta: list[tuple[str, str, str]] = []

    def add(label: str, src: str, alias: str, expr_override: str | None = None) -> None:
        sql_expr = expr_override or f"CAST({src} AS VARCHAR)"
        selects.append(f"COUNT(DISTINCT NULLIF(TRIM({sql_expr}), '')) AS {alias}")
        field_meta.append((label, src, alias))

    add("User Agent",          cfg.col_ua,      "d_ua")
    add("ASN",                 cfg.col_asn,     "d_asn")
    add("City",                cfg.col_city,    "d_city")
    add("State / Region",      cfg.col_state,   "d_state")
    add("Country",             cfg.col_country, "d_country")
    add("Request Host",        cfg.col_reqhost, "d_reqhost")
    add("Client IP",           cfg.col_ip,      "d_ip")

    if qs in cols:
        for param, alias, label, expr_override in [
            ("channel",       "d_qs_channel",  "QS channel (normalised)",  channel_clean_expr(qs)),
            ("platform",      "d_qs_platform", "QS platform",              None),
            ("device",        "d_qs_device",   "QS device",                None),
            ("category_name", "d_qs_category", "QS category",              None),
            ("content_title", "d_qs_content",  "QS content title",         None),
            ("device_id",     "d_qs_devid",    "QS device_id",             None),
            ("session_id",    "d_qs_sessid",   "QS session_id",            None),
        ]:
            col_expr_sql = expr_override if expr_override else qs_extract(qs, param)
            selects.append(f"COUNT(DISTINCT NULLIF(TRIM({col_expr_sql}), '')) AS {alias}")
            field_meta.append((label, f"queryStr[{param}]", alias))

    title_row(ws, 0, "📊  Distinct Value Counts (single-pass DuckDB scan)", 4, s)
    for c, h in enumerate(["Field", "Source Column", "Distinct Values", "In lake?"]):
        ws.write(1, c, h, s.hdr)

    present_selects, present_meta = [], []
    for (lbl, src, alias), sel in zip(field_meta, selects):
        bare_col = src.split("[")[0]
        if bare_col in cols:
            present_selects.append(sel)
            present_meta.append((lbl, src, alias))

    counts: dict[str, int] = {}
    if present_selects:
        res = safe_query(con, f"SELECT {', '.join(present_selects)} FROM {expr}", "distinct counts")
        if not res.empty:
            counts = {k: int(v) for k, v in res.iloc[0].to_dict().items()}

    all_meta = present_meta + [(lbl, src, alias) for (lbl, src, alias) in field_meta if (src.split("[")[0]) not in cols]
    for i, (lbl, src, alias) in enumerate(all_meta):
        is_alt  = i % 2 == 1
        in_lake = src.split("[")[0] in cols
        count_v = counts.get(alias, "—  (column absent)")
        ws.write(i + 2, 0, lbl,        s.alt if is_alt else s.data)
        ws.write(i + 2, 1, src,        s.alt if is_alt else s.data)
        ws.write(i + 2, 2, count_v,    s.num_alt if (is_alt and in_lake) else s.num if in_lake else s.alt if is_alt else s.data)
        ws.write(i + 2, 3, "✓" if in_lake else "✗  missing", s.alt if is_alt else s.data)
    ws.freeze_panes(2, 0)


# ═══════════════════════════════════════════════════════════════
# SHEET 3 — CHANNEL × PLATFORM
# ═══════════════════════════════════════════════════════════════

def build_channel_platform(
    wb:   xlsxwriter.Workbook,
    con:  duckdb.DuckDBPyConnection,
    cfg:  ReportConfig,
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

    expr = lake_expr(cfg)
    df = safe_query(con, f"""
        SELECT
            {channel_clean_expr(qs)}                              AS channel,
            COALESCE({qs_extract(qs,'platform')}, 'Unknown')     AS platform,
            COUNT(*)                                              AS requests,
            COUNT(DISTINCT {qs_extract(qs,'device_id')})         AS devices,
            COUNT(DISTINCT {qs_extract(qs,'session_id')})        AS sessions
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
# MATRIX SHEET (IST date headers)
# ═══════════════════════════════════════════════════════════════

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
    cfg:       ReportConfig,
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
    expr = lake_expr(cfg)
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
    utc_dates = sorted(matrix_data.keys(), reverse=True)   # UTC date strings "YYYY-MM-DD"
    n_dates = len(utc_dates)
    row_order = list(top_df["value"])
    for c in range(1, n_dates + 1):
        ws.set_column(c, c, 12)

    row = 0
    title_row(ws, row, f"📅  {label} Matrix — top {n_vals:,} values × last {n_dates} days (IST dates)", n_dates + 1, s)
    row += 1

    # Convert UTC date strings to IST date strings for headers
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
# MAIN
# ═══════════════════════════════════════════════════════════════

def main() -> None:
    print("\n" + "═" * 58)
    print("  Lake Report Generator  (IST dates, PyArrow + DuckDB + XlsxWriter)")
    print("═" * 58 + "\n")

    if len(sys.argv) > 1:
        lake_root = Path(sys.argv[1]).resolve()
        if not lake_root.exists():
            log.error(f"Folder not found: {lake_root}")
            sys.exit(1)
    else:
        lake_root = prompt_lake_folder()

    cfg = auto_config(lake_root)

    # OPTIONAL: Override matrix_top_n here (increase if memory permits)
    # cfg.matrix_top_n = 50_000   # example

    cfg.output_path      = prompt_output_path(cfg.output_path)
    cfg.year, cfg.month  = prompt_month_filter(lake_root)

    if not prompt_confirm(cfg):
        print("  Cancelled.")
        sys.exit(0)

    log.info("Detecting schema via PyArrow (reading Parquet footers) …")
    schema_map = pa_schema(lake_root, cfg.year, cfg.month)
    cols: set[str] = set(schema_map.keys())
    log.info(f"  {len(cols)} columns: {', '.join(sorted(cols))}")

    ts = cfg.col_ts

    log.info("Connecting to DuckDB …")
    con = duck_conn(cfg)

    log.info("Creating Excel workbook (constant_memory=True) …")
    wb = xlsxwriter.Workbook(str(cfg.output_path), {"constant_memory": True, "strings_to_urls": False})
    s = Styles(wb)

    build_overview(wb, con, cfg, cols, s, ts)
    # build_distinct_counts(wb, con, cfg, cols, s)   # uncomment if needed
    build_channel_platform(wb, con, cfg, cols, s)

    direct_fields = [
        (cfg.col_ua,      "User Agent"),
        (cfg.col_asn,     "ASN"),
        (cfg.col_city,    "City"),
        (cfg.col_state,   "State"),
        (cfg.col_country, "Country"),
        (cfg.col_reqhost, "Request Host"),
        (cfg.col_ip,      "Client IP"),
    ]
    for col_name, label in direct_fields:
        if col_name in cols:
            build_matrix(wb, con, cfg, cols, s, col_name, label)
        else:
            log.info(f"Skipping {label} matrix — column '{col_name}' not in lake")

    qs = cfg.col_qs
    if qs in cols:
        qs_fields = [
            (channel_clean_expr(qs),              "Channel"),
            (qs_extract(qs, "platform"),          "Platform"),
            (qs_extract(qs, "device"),            "Device"),
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