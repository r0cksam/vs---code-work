#!/usr/bin/env python3
"""
lake_report_final.py
═══════════════════════════════════════════════════════════════════════
Production-grade Excel report generator for Hive-partitioned Parquet lakes.

Tool roles (why each library is used):
  PyArrow  — zero-scan metadata reads: schema detection, exact row counts
             from Parquet file footers, partition directory discovery.
             Never used for aggregation — that is always DuckDB's job.
  DuckDB   — all SQL aggregation: GROUP BY, COUNT DISTINCT, matrix pivots,
             queryString regex extraction. Uses hive_partitioning=true for
             partition pruning (skips irrelevant year=/month=/day= dirs).
  XlsxWriter — streaming Excel output with constant_memory=True so RAM
             usage stays flat regardless of how many rows are written.
             merge_range() is intentionally avoided (incompatible with
             constant_memory); title rows are painted cell-by-cell instead.
  psutil   — auto-scales DuckDB memory/thread limits to available RAM.

Usage:
    pip install duckdb pyarrow pandas xlsxwriter psutil
    python lake_report_final.py
    python lake_report_final.py "Z:\\05 Veto Logs\\lake"

Sheets:
    1.  Overview          — report metadata + enhanced day-by-day breakdown
    2.  Distinct Counts   — per-field distinct count summary
    3.  Channel×Platform  — channel/platform cross pivot
    4–9. <Field>_Matrix   — UA, ASN, City, State, Country, reqHost
         top N values × last M days heatmap
   10–14. <QS>_Matrix     — Channel, Platform, Device, Category, Content
         extracted from queryString, same matrix format
"""

from __future__ import annotations

import logging
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
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
    temp_dir:     Optional[Path] = None     # DuckDB spill-to-disk (auto = local %TEMP%)

    # Column names in your lake (set to None if a column doesn't exist)
    col_ua:       str = "UA"
    col_asn:      str = "asn"
    col_city:     str = "city"
    col_state:    str = "state"
    col_country:  str = "country"
    col_reqhost:  str = "reqHost"
    col_qs:       str = "queryStr"
    col_ip:       str = "cliIP"
    col_ts:       str = "reqTimeSec"

    # Injected lake partition columns — excluded from distinct-value analysis
    _partition_cols: frozenset = field(
        default_factory=lambda: frozenset({"year", "month", "day"}),
        repr=False,
    )


# ═══════════════════════════════════════════════════════════════
# AUTO-SCALE CONFIG FROM SYSTEM RESOURCES
# ═══════════════════════════════════════════════════════════════
def auto_config(lake_root: Path) -> ReportConfig:
    """Probe system resources and return a sensibly scaled ReportConfig."""
    mem   = psutil.virtual_memory()
    total = mem.total  / (1024 ** 3)
    free  = mem.available / (1024 ** 3)
    cpus  = os.cpu_count() or 4

    # Use 70% of free RAM, cap based on system size
    mem_gb = max(4, min(int(free * 0.70), 48))

    # Conservative thread count: 1 DuckDB thread per 4 logical cores, max 4
    threads = max(1, min(cpus // 4, 4))

    # Lower matrix limits on small-memory machines
    if total < 16:
        top_n, days = 100_000, 30
    elif total < 32:
        top_n, days = 100_000, 45
    else:
        top_n, days = 100_000, 60

    # Hard cap at Excel's max rows minus header/total
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
    """Ask for optional year/month filter. Validates each field before moving on."""
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

        # ── Year ──────────────────────────────────────────────
        while True:
            year_in = input("  Year (e.g. 2026): ").strip()
            if not year_in.isdigit():
                print(f"  ❌ '{year_in}' is not a number — enter the year as digits e.g. 2026")
                continue
            y = int(year_in)
            if not (2000 <= y <= 2100):
                print("  ❌ Year out of range (2000–2100)")
                continue
            if available_years and year_in not in available_years:
                print(f"  ⚠️  year={year_in} not found. Available: {', '.join(available_years)}")
                print("  Continue anyway? (y/n): ", end="")
                if input().strip().lower() != "y":
                    continue
            break

        # ── Month ─────────────────────────────────────────────
        while True:
            month_in = input("  Month 1–12 (e.g. 5): ").strip()
            if not month_in.isdigit():
                print(f"  ❌ '{month_in}' is not a number — enter a month number e.g. 5")
                continue
            m = int(month_in)
            if not (1 <= m <= 12):
                print("  ❌ Month must be between 1 and 12")
                continue
            break

        year_str  = str(y)
        month_str = f"{m:02d}"

        partition = lake_root / f"year={year_str}" / f"month={month_str}"
        if not partition.exists():
            print(f"  ⚠️  Partition not found on disk: {partition}")
            print("  Continue anyway? (y/n): ", end="")
            if input().strip().lower() != "y":
                return None, None

        log.info(f"Month filter: year={year_str}, month={month_str}")
        return year_str, month_str

    except KeyboardInterrupt:
        print("\n  Cancelled — no month filter applied.")
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
# PYARROW HELPERS  — zero-scan metadata operations
# ═══════════════════════════════════════════════════════════════

def pa_schema(lake_root: Path, year: Optional[str] = None, month: Optional[str] = None) -> dict[str, str]:
    """
    Read column names + types from Parquet file footers — zero data scan.

    PyArrow reads only the footer metadata (a few KB per file) to get the
    schema. This is ~100x faster than DuckDB's DESCRIBE SELECT * ... LIMIT 0
    because no query plan is built and no data pages are touched.

    Partition columns (year, month, day) are excluded since they are synthetic
    path-derived columns, not real data columns.
    """
    scan_root = lake_root
    if year:
        scan_root = lake_root / f"year={year}"
        if month:
            scan_root = scan_root / f"month={month}"

    schema_map: dict[str, str] = {}
    _partition_skip = {"year", "month", "day"}

    for parquet_file in scan_root.rglob("*.parquet"):
        try:
            s = pq.read_schema(parquet_file)          # reads footer only — no row data
            for name in s.names:
                if name in _partition_skip:
                    continue
                if name not in schema_map:
                    schema_map[name] = str(s.field(name).type)
        except Exception as exc:
            log.warning(f"Could not read schema from {parquet_file.name}: {exc}")
        # Stop after first file per day-partition (schemas are homogeneous within a month)
        if len(schema_map) > 5:          # we have enough columns — don't scan every file
            break

    return schema_map


def pa_row_counts_by_day(
    lake_root: Path,
    year: Optional[str] = None,
    month: Optional[str] = None,
) -> dict[str, int]:
    """
    Read exact row counts per day from Parquet file metadata — zero data scan.

    Each Parquet file stores the total row count in its footer. PyArrow can
    read this in microseconds per file without opening any data pages.
    This populates the 'Total Rows' column in the Overview without DuckDB.

    Returns: {"2026-05-01": 1234567, "2026-05-02": ...}
    """
    scan_root = lake_root
    if year:
        scan_root = lake_root / f"year={year}"
        if month:
            scan_root = scan_root / f"month={month}"

    day_counts: dict[str, int] = {}

    for day_dir in sorted(scan_root.rglob("day=*")):
        if not day_dir.is_dir():
            continue
        # Parse date from path components  e.g. year=2026/month=05/day=01
        try:
            parts   = {p.split("=")[0]: p.split("=")[1] for p in day_dir.parts if "=" in p}
            y, mo   = parts.get("year", "0"), parts.get("month", "0")
            d       = parts.get("day", "0")
            date_key = f"{y}-{mo}-{d}"
            total   = 0
            for pf in day_dir.glob("*.parquet"):
                try:
                    meta  = pq.read_metadata(pf)         # reads file footer only
                    total += meta.num_rows
                except Exception:
                    pass
            day_counts[date_key] = day_counts.get(date_key, 0) + total
        except Exception as exc:
            log.warning(f"Row count skip {day_dir}: {exc}")

    return day_counts


def pa_total_file_count(lake_root: Path) -> int:
    """Count parquet files using directory walk — no data access."""
    return sum(1 for _ in lake_root.rglob("*.parquet"))


# ═══════════════════════════════════════════════════════════════
# DUCKDB HELPERS  — all SQL aggregation lives here
# ═══════════════════════════════════════════════════════════════

def duck_conn(cfg: ReportConfig) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET threads={cfg.threads};")
    con.execute(f"SET memory_limit='{cfg.memory_gb}GB';")
    con.execute("SET preserve_insertion_order=false;")

    # Spill-to-disk: DuckDB uses this when it can't fit data in memory.
    # MUST be a local fast drive — never a network share (defeats the purpose).
    # Auto-detect: prefer %TEMP% on Windows, /tmp on Linux/Mac.
    if cfg.temp_dir:
        td = cfg.temp_dir
    else:
        td = Path(os.environ.get("TEMP", os.environ.get("TMP", "/tmp"))) / "duckdb_lake_report"
    td.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory='{td.as_posix()}';")
    log.info(f"DuckDB temp/spill dir: {td}")
    return con


def lake_expr(cfg: ReportConfig) -> str:
    """
    Build the DuckDB table expression.

    When a month filter is set we point DuckDB directly at the partition
    sub-directory instead of wrapping in a subquery. This is critical:
      ✗  (SELECT * FROM reader WHERE year=X AND month=Y)
         → DuckDB scans all files first, then filters — no pruning.
      ✓  read_parquet('lake/year=X/month=Y/**/*.parquet', union_by_name=true)
         → DuckDB opens only those files — true partition pruning.
    """
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
    """SQL expression to extract one query-string parameter, NULL if absent/empty."""
    return f"NULLIF(regexp_extract({qs_col}, '(?:^|&){param}=([^&]+)', 1), '')"


# Platform/device suffixes that get appended to channel names by player apps.
# Stripping these lets us group  9XM + 9XM_FireTV + 9XM_AndroidTV → 9XM.
_PLATFORM_SUFFIX_PATTERN = (
    "_(firetv|firestick|fireos|androidtv|android|webos|web|"
    "ios|iphone|ipad|appletv|apple|samsung|samsungtv|tizen|"
    "roku|mi|mitv|xiaomi|sony|bravia|lg|lgtv|"
    "mobile|phone|tablet|tv)$"
)


def channel_clean_expr(qs_col: str) -> str:
    """
    SQL expression that extracts the channel name from queryString and
    normalises it so platform-suffixed variants merge into one channel.

    Steps applied in SQL:
      1. Extract raw channel from queryStr (prefer 'channel', fall back to 'channel_name')
      2. URL-decode  (%20 → space, etc.)
      3. Strip trailing platform/device suffix  (_FireTV, _AndroidTV, _Android …)
      4. Trim whitespace and collapse to a clean name

    Example:  'B4U_Bhojpuri_FireTV'  →  'B4U_Bhojpuri'
              'Manorama%20_FireTV'   →  'Manorama'
              'NewsNation_BRJH_FireTV' → 'NewsNation_BRJH'
    """
    raw = (
        f"COALESCE("
        f"  {qs_extract(qs_col, 'channel')},"
        f"  {qs_extract(qs_col, 'channel_name')},"
        f"  'Unknown'"
        f")"
    )
    # url_decode handles %20 etc.; regexp_replace strips the platform suffix
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
# XLSXWRITER STYLE FACTORY
# ═══════════════════════════════════════════════════════════════

class Styles:
    """
    Pre-build all formats once and cache them.

    XlsxWriter limits workbooks to 64,000 format objects.
    Creating a new format per cell would exhaust this quickly.
    """

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
                         font_color="#FFFFFF", bg_color="#1F3864",
                         align="left")
        self.hdr    = _f(bold=True, font_color="#FFFFFF", bg_color="#2E75B6",
                         align="center", text_wrap=True)
        self.kv_key = _f(bold=True, font_color="#000000", bg_color="#D6E4F0",
                         align="left")
        self.kv_val = _f(font_color="#000000", bg_color="#FFFFFF", align="left")
        self.data   = _f(font_color="#000000", bg_color="#FFFFFF", align="left")
        self.alt    = _f(font_color="#000000", bg_color="#F2F2F2", align="left")
        self.num    = _f(font_color="#000000", bg_color="#FFFFFF", align="right",
                         num_format="#,##0")
        self.num_alt= _f(font_color="#000000", bg_color="#F2F2F2", align="right",
                         num_format="#,##0")
        self.pct    = _f(font_color="#000000", bg_color="#FFFFFF", align="right",
                         num_format="0.00%")
        self.pct_alt= _f(font_color="#000000", bg_color="#F2F2F2", align="right",
                         num_format="0.00%")
        self.total  = _f(bold=True, font_color="#FFFFFF", bg_color="#E36209",
                         align="right", num_format="#,##0")
        self.total_l= _f(bold=True, font_color="#FFFFFF", bg_color="#E36209",
                         align="left")


def title_row(ws, row: int, text: str, n_cols: int, s: Styles) -> None:
    """
    Write a full-width title row without merge_range().

    merge_range() is disabled in constant_memory mode (XlsxWriter limitation).
    Instead we write the text in col 0 and paint empty cells across the rest
    with the same style — visually identical to a merged cell.
    """
    ws.write(row, 0, text, s.title)
    for c in range(1, n_cols):
        ws.write(row, c, "", s.title)


def kv(ws, row: int, key: str, val, s: Styles) -> None:
    ws.write(row, 0, key, s.kv_key)
    ws.write(row, 1, str(val), s.kv_val)


# ═══════════════════════════════════════════════════════════════
# SHEET 1 — OVERVIEW
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
    for c in range(2, 13):          # increased range because we have one more column
        ws.set_column(c, c, 16)

    row = 0

    # ── Metadata header (unchanged) ──────────────────────────
    title_row(ws, row, "📊  Report Metadata", 2, s); row += 1
    kv(ws, row, "Report generated",
       datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"), s); row += 1

    if cfg.year:
        kv(ws, row, "Filtered to", f"year={cfg.year}, month={cfg.month}", s); row += 1

    kv(ws, row, "Lake folder",    str(cfg.lake_root), s); row += 1
    kv(ws, row, "DuckDB version", duckdb.__version__,  s); row += 1

    # ── Fast row-count total from PyArrow metadata ─────────────
    log.info("  Reading row counts from Parquet metadata (zero-scan) …")
    day_counts = pa_row_counts_by_day(cfg.lake_root, cfg.year, cfg.month)
    total_rows_pa = sum(day_counts.values())
    total_files   = pa_total_file_count(
        cfg.lake_root / (f"year={cfg.year}" if cfg.year else "")
        if cfg.year else cfg.lake_root
    )
    kv(ws, row, "Total rows (metadata)",  f"{total_rows_pa:,}",  s); row += 1
    kv(ws, row, "Total parquet files",    f"{total_files:,}",     s); row += 1

    # ── Date/time range from DuckDB (needs actual data read) ───
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
            kv(ws, row, "Data date range",
               f"{r['min_date']}  →  {r['max_date']}", s); row += 1
            kv(ws, row, "Data time range",
               f"{str(r['min_ts'])[:19]}  →  {str(r['max_ts'])[:19]}", s); row += 1
    row += 1

    # ── Day-by-day breakdown ────────────────────────────────────
    N_COLS = 13                     # increased from 12 to 13 columns
    title_row(ws, row, "📅  Day-by-Day Breakdown", N_COLS, s); row += 1

    hdr_labels = [
        "Date", "Total Rows*", "cliIP rows", "Distinct cliIP",
        "Total Bytes (GiB)",                                 # new column
        "Rows w/ session_id", "Distinct session_id",
        "Rows w/ device_id",  "Distinct device_id",
        "device_id blank",    "session_id blank",
        "device_id missing",  "session_id missing",
    ]
    for c, h in enumerate(hdr_labels):
        ws.write(row, c, h, s.hdr)
    row += 1

    if ts in cols:
        qs      = cfg.col_qs
        ip_col  = cfg.col_ip
        has_qs  = qs in cols
        has_ip  = ip_col in cols

        ip_rows     = f"COUNT({ip_col})"          if has_ip else "0"
        d_ip        = f"COUNT(DISTINCT {ip_col})" if has_ip else "0"

        # Check if totalBytes column exists
        total_bytes_col = "totalBytes" if "totalBytes" in cols else None
        if total_bytes_col:
            sum_bytes = f"SUM(CAST({total_bytes_col} AS BIGINT))"
        else:
            sum_bytes = "NULL"

        if has_qs:
            sess_present  = f"SUM(CASE WHEN {qs} LIKE '%session_id=%' THEN 1 ELSE 0 END)"
            dev_present   = f"SUM(CASE WHEN {qs} LIKE '%device_id=%' THEN 1 ELSE 0 END)"
            sess_distinct = f"COUNT(DISTINCT {qs_extract(qs, 'session_id')})"
            dev_distinct  = f"COUNT(DISTINCT {qs_extract(qs, 'device_id')})"
            dev_blank     = (f"SUM(CASE WHEN {qs} LIKE '%device_id=%'"
                             f" AND {qs_extract(qs,'device_id')} IS NULL THEN 1 ELSE 0 END)")
            sess_blank    = (f"SUM(CASE WHEN {qs} LIKE '%session_id=%'"
                             f" AND {qs_extract(qs,'session_id')} IS NULL THEN 1 ELSE 0 END)")
            dev_missing   = f"SUM(CASE WHEN {qs} IS NULL OR {qs} NOT LIKE '%device_id=%' THEN 1 ELSE 0 END)"
            sess_missing  = f"SUM(CASE WHEN {qs} IS NULL OR {qs} NOT LIKE '%session_id=%' THEN 1 ELSE 0 END)"
        else:
            sess_present = sess_distinct = dev_present = dev_distinct = "0"
            dev_blank = sess_blank = dev_missing = sess_missing = "0"

        expr    = lake_expr(cfg)
        day_df  = safe_query(con, f"""
            SELECT
                make_date(CAST(year AS INT), CAST(month AS INT), CAST(day AS INT)) AS date,
                COUNT(*)         AS total_rows,
                {ip_rows}        AS ip_rows,
                {d_ip}           AS distinct_ip,
                {sum_bytes}      AS total_bytes,
                {sess_present}   AS sess_rows,
                {sess_distinct}  AS distinct_sess,
                {dev_present}    AS dev_rows,
                {dev_distinct}   AS distinct_dev,
                {dev_blank}      AS dev_blank,
                {sess_blank}     AS sess_blank,
                {dev_missing}    AS dev_missing,
                {sess_missing}   AS sess_missing
            FROM {expr}
            GROUP BY year, month, day
            ORDER BY year, month, day
        """, "day breakdown")

        data_start = row
        for _, r in day_df.iterrows():
            dt       = r["date"]
            date_str = dt.strftime("%d/%m/%y") if hasattr(dt, "strftime") else str(dt)
            is_alt   = (row - data_start) % 2 == 1
            pa_count = day_counts.get(str(dt), int(r["total_rows"]))

            # Convert bytes to GiB (2^30) for readability
            bytes_val = r["total_bytes"] if total_bytes_col and pd.notna(r["total_bytes"]) else 0
            gb_val = bytes_val / (1024 ** 3) if bytes_val else 0

            vals = [
                date_str, pa_count,
                int(r["ip_rows"]),     int(r["distinct_ip"]),
                round(gb_val, 2),                      # new column: GiB
                int(r["sess_rows"]),   int(r["distinct_sess"]),
                int(r["dev_rows"]),    int(r["distinct_dev"]),
                int(r["dev_blank"]),   int(r["sess_blank"]),
                int(r["dev_missing"]), int(r["sess_missing"]),
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
                ws.write(
                    row, c,
                    f"=SUM({col_l}{data_start + 1}:{col_l}{data_end})",
                    s.total,
                )
            row += 1

    ws.write(row + 1, 0,
             "* Total Rows sourced from Parquet file metadata (zero-scan). "
             "All other columns via DuckDB aggregation.", s.data)
    ws.freeze_panes(2, 1) 
# ═══════════════════════════════════════════════════════════════
# SHEET 2 — DISTINCT COUNTS SUMMARY
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

    # Build one SELECT that counts all distinct values in a single DuckDB pass
    selects: list[str] = []
    field_meta: list[tuple[str, str, str]] = []     # (label, source_col, select_alias)

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
            selects.append(
                f"COUNT(DISTINCT NULLIF(TRIM({col_expr_sql}), '')) AS {alias}"
            )
            field_meta.append((label, f"queryStr[{param}]", alias))

    title_row(ws, 0, "📊  Distinct Value Counts (single-pass DuckDB scan)", 4, s)
    for c, h in enumerate(["Field", "Source Column", "Distinct Values", "In lake?"]):
        ws.write(1, c, h, s.hdr)

    # Filter out fields whose source columns are absent from the lake
    present_selects, present_meta = [], []
    for (lbl, src, alias), sel in zip(field_meta, selects):
        bare_col = src.split("[")[0]   # strip [param] from QS fields
        if bare_col in cols:
            present_selects.append(sel)
            present_meta.append((lbl, src, alias))

    counts: dict[str, int] = {}
    if present_selects:
        res = safe_query(
            con,
            f"SELECT {', '.join(present_selects)} FROM {expr}",
            "distinct counts",
        )
        if not res.empty:
            counts = {k: int(v) for k, v in res.iloc[0].to_dict().items()}

    # Write absent fields first (greyed), then present
    all_meta = present_meta + [
        (lbl, src, alias)
        for (lbl, src, alias) in field_meta
        if (src.split("[")[0]) not in cols
    ]
    for i, (lbl, src, alias) in enumerate(all_meta):
        is_alt  = i % 2 == 1
        in_lake = src.split("[")[0] in cols
        count_v = counts.get(alias, "—  (column absent)")
        ws.write(i + 2, 0, lbl,        s.alt if is_alt else s.data)
        ws.write(i + 2, 1, src,        s.alt if is_alt else s.data)
        ws.write(i + 2, 2, count_v,    s.num_alt if (is_alt and in_lake) else
                                        s.num    if in_lake else
                                        s.alt    if is_alt  else s.data)
        ws.write(i + 2, 3, "✓" if in_lake else "✗  missing",
                            s.alt if is_alt else s.data)

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

    for c, h in enumerate(["Channel", "Platform", "Requests", "Unique Devices",
                            "Unique Sessions", "% of Requests"]):
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
# MATRIX SHEET — top-N values × last-M days heatmap
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
        # Use iterdir() instead of glob() – works on UNC
        for child in month_dir.iterdir():
            if not child.is_dir():
                continue
            if not child.name.startswith("day="):
                continue
            d = child.name[4:]   # remove "day=" prefix
            days.append((f"{year}-{month}-{d}", child))
        # Sort newest first (descending)
        days.sort(key=lambda x: x[0], reverse=True)
        return days[:max_days]

    elif year:
        year_dir = lake_root / f"year={year}"
        if not year_dir.exists():
            return []
        for month_dir in sorted(year_dir.iterdir(), reverse=True):
            if not month_dir.is_dir() or not month_dir.name.startswith("month="):
                continue
            mo = month_dir.name[6:]   # remove "month="
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
            y = year_dir.name[5:]   # remove "year="
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
    """
    Build the matrix data by querying ONE day partition at a time.

    Uses a wildcard pattern for each day directory to avoid UNC path issues.
    """
    results: dict[str, dict[str, int]] = {}

    for i, (date_str, day_dir) in enumerate(day_parts):
        # Use forward slashes and wildcard – works reliably on Windows UNC
        pattern = f"{day_dir.as_posix()}/*.parquet"
        # Check if any parquet files exist (fast check via glob)
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

        # Filter in Python
        day_dict: dict[str, int] = {
            str(r.value): int(r.cnt)
            for r in df.itertuples(index=False)
            if str(r.value) in top_vals
        }
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

    # ── Step 1: discover day partitions ──────────────────────────
    day_parts = _discover_day_partitions(
        cfg.lake_root, cfg.year, cfg.month, cfg.matrix_days
    )
    if not day_parts:
        ws.write(0, 0, f"No day partitions found.", s.data)
        return
    log.info(f"  {len(day_parts)} day partitions found (last {cfg.matrix_days} days)")

    # ── Step 2: top N values – respect Excel row limit ───────────
    EXCEL_MAX_DATA_ROWS = 1_048_576 - 5   # leave room for header + total row
    safe_top_n = min(cfg.matrix_top_n, EXCEL_MAX_DATA_ROWS)
    log.info(f"  Fetching top {safe_top_n:,} values …")
    expr    = lake_expr(cfg)
    top_df  = safe_query(con, f"""
        SELECT
            COALESCE(CAST({col_expr} AS VARCHAR), '(null)') AS value,
            COUNT(*) AS total
        FROM {expr}
        WHERE {col_expr} IS NOT NULL
          AND TRIM(CAST({col_expr} AS VARCHAR)) <> ''
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT {safe_top_n + 1}   -- fetch one extra to detect truncation
    """, f"top_{label}")

    if top_df.empty:
        ws.write(0, 0, f"No data for {label}.", s.data)
        return

    # Check if we truncated
    truncated = False
    if len(top_df) > safe_top_n:
        truncated = True
        top_df = top_df.iloc[:safe_top_n]   # keep only safe_top_n rows

    n_vals   = len(top_df)
    top_vals = set(top_df["value"].tolist())
    log.info(f"  {n_vals:,} distinct values found" + (" (truncated to Excel row limit)" if truncated else ""))

    # ── Step 3: day-by-day counts ───────────────────────────────
    log.info(f"  Computing daily counts (day-by-day, OOM-safe) …")
    matrix_data = _matrix_counts_by_day(con, day_parts, col_expr, top_vals, label)

    if not matrix_data:
        ws.write(0, 0, f"No daily data for {label}.", s.data)
        return

    # ── Step 4: build pivot ─────────────────────────────────────
    log.info(f"  Pivoting …")
    dates   = sorted(matrix_data.keys(), reverse=True)   # newest first
    n_dates = len(dates)

    # Sort rows by total desc (matches top_df order)
    row_order = list(top_df["value"])

    for c in range(1, n_dates + 1):
        ws.set_column(c, c, 12)

    # ── Step 5: write to Excel ─────────────────────────────────
    row = 0
    title_row(ws, row,
              f"📅  {label} Matrix — top {n_vals:,} values × last {n_dates} days",
              n_dates + 1, s)
    row += 1

    headers = [label] + [d for d in dates]
    for c, h in enumerate(headers):
        ws.write(row, c, h, s.hdr)
    row += 1

    data_start = row
    log.info(f"  Writing {n_vals:,} rows …")
    for idx, val in enumerate(row_order):
        is_alt = idx % 2 == 1
        d, n   = (s.alt, s.num_alt) if is_alt else (s.data, s.num)
        ws.write(row, 0, val, d)
        for c, date in enumerate(dates, 1):
            v = matrix_data.get(date, {}).get(val, 0)
            ws.write(row, c, v, n)
        row += 1
        if (idx + 1) % 10_000 == 0:
            log.info(f"    {idx + 1:,} rows written …")

    # Total row
    ws.write(row, 0, "TOTAL", s.total_l)
    for c in range(1, n_dates + 1):
        col_l = xlsxwriter.utility.xl_col_to_name(c)
        ws.write(row, c,
                 f"=SUM({col_l}{data_start + 1}:{col_l}{row})",
                 s.total)
    row += 1

    # Warning if truncated
    if truncated:
        warning_msg = f"⚠️  Only top {safe_top_n:,} values shown (Excel row limit). {len(top_df) - safe_top_n} values omitted."
        ws.write(row, 0, warning_msg, s.data)
        # Paint the whole row in light red for attention
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
    print("  Lake Report Generator  (PyArrow + DuckDB + XlsxWriter)")
    print("═" * 58 + "\n")

    # ── 1. Resolve lake root ───────────────────────────────────
    if len(sys.argv) > 1:
        lake_root = Path(sys.argv[1]).resolve()
        if not lake_root.exists():
            log.error(f"Folder not found: {lake_root}")
            sys.exit(1)
    else:
        lake_root = prompt_lake_folder()

    cfg = auto_config(lake_root)

    # ── 2. Interactive prompts ────────────────────────────────
    cfg.output_path      = prompt_output_path(cfg.output_path)
    cfg.year, cfg.month  = prompt_month_filter(lake_root)

    if not prompt_confirm(cfg):
        print("  Cancelled.")
        sys.exit(0)

    # ── 3. Schema detection via PyArrow (zero scan) ───────────
    log.info("Detecting schema via PyArrow (reading Parquet footers) …")
    schema_map = pa_schema(lake_root, cfg.year, cfg.month)
    cols: set[str] = set(schema_map.keys())
    log.info(f"  {len(cols)} columns: {', '.join(sorted(cols))}")

    ts = cfg.col_ts

    # ── 4. DuckDB connection ───────────────────────────────────
    log.info("Connecting to DuckDB …")
    con = duck_conn(cfg)

    # ── 5. Build workbook ─────────────────────────────────────
    log.info("Creating Excel workbook (constant_memory=True) …")
    wb = xlsxwriter.Workbook(
        str(cfg.output_path),
        {"constant_memory": True, "strings_to_urls": False},
    )
    s = Styles(wb)

    # Sheet 1 — Overview
    build_overview(wb, con, cfg, cols, s, ts)

    # Sheet 2 — Distinct Counts
    # build_distinct_counts(wb, con, cfg, cols, s)

    # Sheet 3 — Channel × Platform
    build_channel_platform(wb, con, cfg, cols, s)

    # Matrix sheets — direct lake columns
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

    # Matrix sheets — queryString-derived
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

    # ── 6. Save ────────────────────────────────────────────────
    log.info(f"Saving → {cfg.output_path}")
    wb.close()
    con.close()

    print(f"\n{'═' * 58}")
    print(f"  ✅  Report saved: {cfg.output_path}")
    print(f"{'═' * 58}\n")


if __name__ == "__main__":
    main()