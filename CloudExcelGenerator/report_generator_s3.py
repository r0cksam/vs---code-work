#!/usr/bin/env python3
"""
report_generator_s3.py - Generate reports directly from Linode S3 (Parquet lake).
Now with comprehensive error handling and progress tracking.
"""

from __future__ import annotations

import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import psutil
import duckdb
import pandas as pd
import xlsxwriter
import xlsxwriter.utility

# Import from the S3‑enabled shared utils
from shared_utils_s3 import (
    get_conn, lake_reader, qs_extract, channel_clean_expr,
    IST_OFFSET, utc_date_to_ist_str, utc_date_str_to_ist_str,
    decode_device, clean_percent_encoding,
    write_to_excel, get_existing_dates, merge_and_rewrite,
    DETAILED_ANALYSIS_FILENAME, DETAILED_SHEET_NAME,
    LAKE_S3_ROOT,
)

log = logging.getLogger("report_generator")
logging.basicConfig(level=logging.INFO, format="  %(levelname)-7s %(message)s")

# ──────────────────────────────────────────────────────────────
# PROMPTS (unchanged)
# ──────────────────────────────────────────────────────────────
def prompt_path(question: str, must_be_dir: bool = True) -> Path:
    """Only used for output folder, not for lake_root."""
    while True:
        raw = input(question).strip()
        if not raw:
            print("  Path cannot be empty.")
            continue
        p = Path(raw).expanduser().resolve()
        if must_be_dir:
            if p.is_dir():
                return p
            print(f"  ❌ Directory not found: {p}")
        else:
            if p.is_file():
                return p
            print(f"  ❌ File not found: {p}")

def prompt_optional(question: str, default: str = "") -> str | None:
    ans = input(question).strip()
    return (default or None) if not ans else ans

def prompt_year_month(s3_root: str) -> tuple[str | None, str | None]:
    """Ask for year/month filter with progress indicator."""
    print("\n📅  Filter by month? (y/n): ", end="")
    if input().strip().lower() != "y":
        return None, None

    print("  🔍 Scanning available partitions from S3 (may take a moment)...", end="", flush=True)
    con = None
    try:
        con = get_conn()
        start = time.time()
        available = con.execute(f"""
            SELECT DISTINCT year, month
            FROM glob('{s3_root}/**/*.parquet')
            WHERE year IS NOT NULL AND month IS NOT NULL
            ORDER BY year DESC, month DESC
        """).fetchall()
        elapsed = time.time() - start
        print(f" done ({elapsed:.1f}s)")
        if available:
            years = sorted(set(y for y, _ in available))
            print(f"  Available years: {', '.join(years)}")
    except Exception as e:
        print(f"\n  ⚠️  Could not list partitions: {e}")
        print("  Continuing without automatic year list.")
    finally:
        if con:
            con.close()

    while True:
        y_in = input("  Year (e.g. 2026): ").strip()
        if not y_in.isdigit() or not (2000 <= int(y_in) <= 2100):
            print("  ❌ Invalid year")
            continue
        break
    while True:
        m_in = input("  Month 1–12: ").strip()
        if not m_in.isdigit() or not (1 <= int(m_in) <= 12):
            print("  ❌ Invalid month")
            continue
        break
    return y_in, f"{int(m_in):02d}"

# ──────────────────────────────────────────────────────────────
# MATRIX REPORT (fully adapted for S3)
# ──────────────────────────────────────────────────────────────
@dataclass
class MatrixConfig:
    lake_root:    str
    output_path:  Path
    year:         Optional[str] = None
    month:        Optional[str] = None
    matrix_top_n: int = 40_000
    matrix_days:  int = 60
    threads:      int = 2
    memory_gb:    int = 8
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

def auto_config(s3_root: str) -> MatrixConfig:
    mem = psutil.virtual_memory()
    total = mem.total / (1024**3)
    free = mem.available / (1024**3)
    cpus = os.cpu_count() or 4
    mem_gb = max(4, min(int(free * 0.70), 48))
    threads = max(1, min(cpus // 4, 4))
    if total < 16:
        top_n, days = 50_000, 30
    elif total < 32:
        top_n, days = 50_000, 45
    else:
        top_n, days = 50_000, 60
    top_n = min(top_n, 1_048_576 - 5)
    log.info(f"System: {cpus} cores | {total:.1f} GiB total | {free:.1f} GiB free")
    log.info(f"DuckDB: {threads} threads | {mem_gb} GiB limit")
    log.info(f"Matrix: top {top_n:,} values × {days} days")
    return MatrixConfig(
        lake_root    = s3_root,
        output_path  = Path.cwd() / "matrix_report.xlsx",
        threads      = threads,
        memory_gb    = mem_gb,
        matrix_top_n = top_n,
        matrix_days  = days,
    )

# ---- Row counts via DuckDB (fast, scans Parquet footers) ----
def get_row_counts_by_day(con: duckdb.DuckDBPyConnection, s3_root: str, year: Optional[str], month: Optional[str]) -> dict[str, int]:
    path = s3_root
    if year:
        path += f"/year={year}"
        if month:
            path += f"/month={month}"
    query = f"""
        SELECT
            make_date(year::INT, month::INT, day::INT) AS utc_date,
            COUNT(*) AS row_count
        FROM read_parquet('{path}/**/*.parquet', hive_partitioning=true)
        GROUP BY year, month, day
    """
    try:
        df = con.execute(query).df()
        return {str(row.utc_date): int(row.row_count) for row in df.itertuples()}
    except Exception as e:
        log.warning(f"Row count query failed: {e}")
        return {}

def get_total_file_count(con: duckdb.DuckDBPyConnection, s3_root: str, year: Optional[str], month: Optional[str]) -> int:
    path = s3_root
    if year:
        path += f"/year={year}"
        if month:
            path += f"/month={month}"
    try:
        result = con.execute(f"""
            SELECT COUNT(*) AS cnt FROM glob('{path}/**/*.parquet')
        """).fetchone()
        return result[0] if result else 0
    except Exception as e:
        log.warning(f"File count query failed: {e}")
        return 0

# ---- Partition discovery via SQL ----
def discover_day_partitions(con: duckdb.DuckDBPyConnection, s3_root: str, year: Optional[str], month: Optional[str], max_days: int) -> list[tuple[str, str]]:
    path = s3_root
    if year:
        path += f"/year={year}"
        if month:
            path += f"/month={month}"
    query = f"""
        SELECT DISTINCT
            year, month, day,
            make_date(year::INT, month::INT, day::INT) AS utc_date
        FROM glob('{path}/**/*.parquet')
        WHERE year IS NOT NULL AND month IS NOT NULL AND day IS NOT NULL
        ORDER BY utc_date DESC
        LIMIT {max_days}
    """
    try:
        df = con.execute(query).df()
        result = []
        for _, row in df.iterrows():
            date_str = f"{int(row.year)}-{int(row.month):02d}-{int(row.day):02d}"
            part_path = f"{s3_root}/year={row.year}/month={row.month}/day={row.day}"
            result.append((date_str, part_path))
        return result
    except Exception as e:
        log.warning(f"Partition discovery failed: {e}")
        return []

# ---- Matrix counts per day (adapted to S3) ----
def matrix_counts_by_day(con, day_parts, col_expr, top_vals, label) -> dict[str, dict[str, int]]:
    results = {}
    total = len(day_parts)
    for i, (date_str, part_path) in enumerate(day_parts):
        reader = f"read_parquet('{part_path}/*.parquet', union_by_name=true)"
        try:
            df = con.execute(f"""
                SELECT
                    COALESCE(CAST({col_expr} AS VARCHAR), '(null)') AS value,
                    COUNT(*) AS cnt
                FROM {reader}
                WHERE {col_expr} IS NOT NULL
                  AND TRIM(CAST({col_expr} AS VARCHAR)) <> ''
                GROUP BY 1
            """).df()
            if df.empty:
                continue
            day_dict = {str(r.value): int(r.cnt) for r in df.itertuples() if str(r.value) in top_vals}
            if day_dict:
                results[date_str] = day_dict
        except Exception as e:
            log.warning(f"Failed for {date_str}: {e}")
        if (i+1) % 10 == 0 or (i+1) == total:
            print(f"    Progress: {i+1}/{total} days processed ...", end="\r" if (i+1) < total else "\n")
    return results

# ---- XlsxWriter styles (copy from your original) ----
class Styles:
    def __init__(self, wb: xlsxwriter.Workbook):
        def _f(**kw):
            base = {
                "font_name": "Arial", "font_size": 10,
                "border": 1, "border_color": "#CCCCCC", "valign": "vcenter",
            }
            base.update(kw)
            return wb.add_format(base)

        self.title   = _f(font_size=11, bold=True, font_color="#FFFFFF",
                          bg_color="#1F3864", align="left")
        self.hdr     = _f(bold=True, font_color="#FFFFFF", bg_color="#2E75B6",
                          align="center", text_wrap=True)
        self.kv_key  = _f(bold=True, font_color="#000000", bg_color="#D6E4F0", align="left")
        self.kv_val  = _f(font_color="#000000", bg_color="#FFFFFF", align="left")
        self.data    = _f(font_color="#000000", bg_color="#FFFFFF", align="left")
        self.alt     = _f(font_color="#000000", bg_color="#F2F2F2", align="left")
        self.num     = _f(font_color="#000000", bg_color="#FFFFFF",
                          align="right", num_format="#,##0")
        self.num_alt = _f(font_color="#000000", bg_color="#F2F2F2",
                          align="right", num_format="#,##0")
        self.pct     = _f(font_color="#000000", bg_color="#FFFFFF",
                          align="right", num_format="0.00%")
        self.pct_alt = _f(font_color="#000000", bg_color="#F2F2F2",
                          align="right", num_format="0.00%")
        self.total   = _f(bold=True, font_color="#FFFFFF", bg_color="#E36209",
                          align="right", num_format="#,##0")
        self.total_l = _f(bold=True, font_color="#FFFFFF", bg_color="#E36209",
                          align="left")

def _title_row(ws, row: int, text: str, n_cols: int, s: Styles) -> None:
    ws.write(row, 0, text, s.title)
    for c in range(1, n_cols):
        ws.write(row, c, "", s.title)

def _kv(ws, row: int, key: str, val, s: Styles) -> None:
    ws.write(row, 0, key, s.kv_key)
    ws.write(row, 1, str(val), s.kv_val)

# ---- Sheet builders (with try/except for each) ----
def build_overview(wb, con, cfg, cols, s):
    print("  📊 Building Overview sheet...", end="", flush=True)
    try:
        log.info("Building Overview …")
        ws = wb.add_worksheet("Overview")
        ws.set_column(0, 0, 30)
        ws.set_column(1, 1, 38)
        for c in range(2, 15):
            ws.set_column(c, c, 16)

        row = 0
        _title_row(ws, row, "📊  Report Metadata", 2, s); row += 1

        ist_now = datetime.now(timezone.utc) + IST_OFFSET
        _kv(ws, row, "Report generated", ist_now.strftime("%Y-%m-%d %H:%M IST"), s); row += 1
        if cfg.year:
            _kv(ws, row, "Filtered to", f"year={cfg.year}, month={cfg.month}", s); row += 1
        _kv(ws, row, "Lake (S3)", cfg.lake_root, s); row += 1
        _kv(ws, row, "DuckDB version", duckdb.__version__, s); row += 1

        log.info("  Reading row counts from Parquet metadata (fast scan) …")
        day_counts = get_row_counts_by_day(con, cfg.lake_root, cfg.year, cfg.month)
        total_rows_pa = sum(day_counts.values())
        total_files = get_total_file_count(con, cfg.lake_root, cfg.year, cfg.month)
        _kv(ws, row, "Total rows (metadata)", f"{total_rows_pa:,}", s); row += 1
        _kv(ws, row, "Total parquet files",   f"{total_files:,}",   s); row += 1

        ts = cfg.col_ts
        if ts in cols:
            expr = lake_reader(cfg.lake_root, cfg.year, cfg.month)
            meta = con.execute(f"""
                SELECT
                    MIN(to_timestamp(CAST({ts} AS DOUBLE)))::DATE AS min_date,
                    MAX(to_timestamp(CAST({ts} AS DOUBLE)))::DATE AS max_date,
                    MIN(to_timestamp(CAST({ts} AS DOUBLE)))       AS min_ts,
                    MAX(to_timestamp(CAST({ts} AS DOUBLE)))       AS max_ts
                FROM {expr}
            """).df()
            if not meta.empty:
                r = meta.iloc[0]
                _kv(ws, row, "Data date range (IST)",
                    f"{(r['min_date'] + IST_OFFSET).strftime('%Y-%m-%d')} → "
                    f"{(r['max_date'] + IST_OFFSET).strftime('%Y-%m-%d')}", s); row += 1
                _kv(ws, row, "Data time range (IST)",
                    f"{(r['min_ts']  + IST_OFFSET).strftime('%Y-%m-%d %H:%M:%S')} → "
                    f"{(r['max_ts']  + IST_OFFSET).strftime('%Y-%m-%d %H:%M:%S')}", s); row += 1
        row += 1

        N_COLS = 15
        _title_row(ws, row, "📅  Day-by-Day Breakdown (IST dates)", N_COLS, s); row += 1
        hdr_labels = [
            "Date (IST)", "Total Rows*", "cliIP rows", "Distinct cliIP",
            "Distinct cliIP & UA", "Total Bytes (GiB)",
            "Rows w/ session_id = 1", "Distinct session_id",
            "session_id = Null", "session_id = NA",
            "Rows w/ device_id = 1", "Distinct device_id",
            "device_id = Null", "device_id = NA",
        ]
        for c, h in enumerate(hdr_labels):
            ws.write(row, c, h, s.hdr)
        row += 1

        if ts in cols:
            qs, ip_col, ua_col = cfg.col_qs, cfg.col_ip, cfg.col_ua
            has_qs  = qs     in cols
            has_ip  = ip_col in cols
            has_ua  = ua_col in cols

            ip_rows       = f"COUNT({ip_col})"         if has_ip else "0"
            d_ip          = f"COUNT(DISTINCT {ip_col})" if has_ip else "0"
            distinct_ipua = f"COUNT(DISTINCT ({ip_col}, {ua_col}))" if (has_ip and has_ua) else "NULL"
            tb_col        = "totalBytes" if "totalBytes" in cols else None
            sum_bytes     = f"SUM(CAST({tb_col} AS BIGINT))" if tb_col else "NULL"

            if has_qs:
                sess_present  = f"SUM(CASE WHEN {qs} LIKE '%session_id=%' THEN 1 ELSE 0 END)"
                dev_present   = f"SUM(CASE WHEN {qs} LIKE '%device_id=%'  THEN 1 ELSE 0 END)"
                sess_distinct = f"COUNT(DISTINCT {qs_extract(qs, 'session_id')})"
                dev_distinct  = f"COUNT(DISTINCT {qs_extract(qs, 'device_id')})"
                dev_blank     = (f"SUM(CASE WHEN {qs} LIKE '%device_id=%' "
                                 f"AND {qs_extract(qs,'device_id')} IS NULL THEN 1 ELSE 0 END)")
                sess_blank    = (f"SUM(CASE WHEN {qs} LIKE '%session_id=%' "
                                 f"AND {qs_extract(qs,'session_id')} IS NULL THEN 1 ELSE 0 END)")
                dev_missing   = f"SUM(CASE WHEN {qs} IS NULL OR {qs} NOT LIKE '%device_id=%'  THEN 1 ELSE 0 END)"
                sess_missing  = f"SUM(CASE WHEN {qs} IS NULL OR {qs} NOT LIKE '%session_id=%' THEN 1 ELSE 0 END)"
                dev_nonblank  = f"({dev_present}  - {dev_blank})"
                sess_nonblank = f"({sess_present} - {sess_blank})"
            else:
                sess_present = sess_distinct = dev_present = dev_distinct = "0"
                dev_blank = sess_blank = dev_missing = sess_missing = "0"
                dev_nonblank = sess_nonblank = "0"

            expr   = lake_reader(cfg.lake_root, cfg.year, cfg.month)
            day_df = con.execute(f"""
                SELECT
                    make_date(CAST(year AS INT), CAST(month AS INT), CAST(day AS INT)) AS utc_date,
                    COUNT(*)        AS total_rows,
                    {ip_rows}       AS ip_rows,
                    {d_ip}          AS distinct_ip,
                    {distinct_ipua} AS distinct_ipua,
                    {sum_bytes}     AS total_bytes,
                    {sess_nonblank} AS sess_nonblank,
                    {sess_distinct} AS distinct_sess,
                    {sess_blank}    AS sess_blank,
                    {sess_missing}  AS sess_missing,
                    {dev_nonblank}  AS dev_nonblank,
                    {dev_distinct}  AS distinct_dev,
                    {dev_blank}     AS dev_blank,
                    {dev_missing}   AS dev_missing
                FROM {expr}
                GROUP BY year, month, day
                ORDER BY utc_date
            """).df()

            data_start = row
            for _, r in day_df.iterrows():
                ist_str  = utc_date_to_ist_str(r["utc_date"])
                is_alt   = (row - data_start) % 2 == 1
                pa_count = day_counts.get(str(r["utc_date"]), int(r["total_rows"]))
                bytes_gib = (
                    (r["total_bytes"] / (1024 ** 3))
                    if tb_col and pd.notna(r["total_bytes"])
                    else 0
                )
                vals = [
                    ist_str, pa_count,
                    int(r["ip_rows"]), int(r["distinct_ip"]),
                    int(r["distinct_ipua"]) if (has_ip and has_ua) else 0,
                    round(bytes_gib, 2),
                    int(r["sess_nonblank"]), int(r["distinct_sess"]),
                    int(r["sess_blank"]),    int(r["sess_missing"]),
                    int(r["dev_nonblank"]),  int(r["distinct_dev"]),
                    int(r["dev_blank"]),     int(r["dev_missing"]),
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
                    ws.write(row, c, f"=SUM({col_l}{data_start+1}:{col_l}{data_end})", s.total)
                row += 1

        ws.write(row + 1, 0,
                 "* Total Rows sourced from Parquet file metadata (fast scan). "
                 "All other columns via DuckDB aggregation.", s.data)
        ws.freeze_panes(2, 1)
        print(" ✓")
    except Exception as e:
        print(f" ❌ Failed: {e}")
        log.error(f"Overview sheet failed: {e}", exc_info=True)

def build_channel_platform(wb, con, cfg, cols, s):
    print("  📡 Building Channel×Platform sheet...", end="", flush=True)
    try:
        log.info("Building Channel×Platform …")
        ws = wb.add_worksheet("Channel×Platform")
        for c, w in enumerate([40, 20, 16, 16, 16, 16]):
            ws.set_column(c, c, w)

        qs = cfg.col_qs
        N  = 6
        _title_row(ws, 0, "📡  Channel × Platform — requests, devices, sessions", N, s)
        if qs not in cols:
            ws.write(1, 0, "queryStr column not found.", s.data)
            print(" ⚠️  skipped (no queryStr)")
            return
        for c, h in enumerate(["Channel", "Platform", "Requests",
                                "Unique Devices", "Unique Sessions", "% of Requests"]):
            ws.write(1, c, h, s.hdr)

        expr = lake_reader(cfg.lake_root, cfg.year, cfg.month)
        df = con.execute(f"""
            SELECT
                {channel_clean_expr(qs)}                          AS channel,
                COALESCE({qs_extract(qs, 'platform')}, 'Unknown') AS platform,
                COUNT(*)                                          AS requests,
                COUNT(DISTINCT {qs_extract(qs, 'device_id')})     AS devices,
                COUNT(DISTINCT {qs_extract(qs, 'session_id')})    AS sessions
            FROM {expr}
            WHERE {qs} IS NOT NULL AND {qs} LIKE '%channel=%'
            GROUP BY 1, 2
            ORDER BY 3 DESC
            LIMIT 5000
        """).df()

        if df.empty:
            ws.write(2, 0, "No channel data found.", s.data)
            print(" ⚠️  no data")
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
            ws.write(i + 2, 5, round(pct, 4), s.pct_alt if is_alt else s.pct)
        ws.freeze_panes(2, 0)
        print(" ✓")
    except Exception as e:
        print(f" ❌ Failed: {e}")
        log.error(f"Channel×Platform failed: {e}", exc_info=True)

def build_matrix(wb, con, cfg, cols, s, col_expr, label):
    print(f"  📈 Building {label} matrix...", end="", flush=True)
    try:
        sheet_name = re.sub(r"[:\[\]*?/\\]", "_", label)[:31] + "_Matrix"
        log.info(f"Building matrix: {label} …")
        ws = wb.add_worksheet(sheet_name)
        ws.set_column(0, 0, 52)

        if cfg.col_ts not in cols:
            ws.write(0, 0, f"reqTimeSec missing — cannot build matrix for {label}", s.data)
            print(" ⚠️  missing reqTimeSec")
            return

        day_parts = discover_day_partitions(con, cfg.lake_root, cfg.year, cfg.month, cfg.matrix_days)
        if not day_parts:
            ws.write(0, 0, "No day partitions found.", s.data)
            print(" ⚠️  no partitions")
            return
        log.info(f"  {len(day_parts)} day partitions, last {cfg.matrix_days} days")

        safe_top_n = min(cfg.matrix_top_n, 1_048_576 - 5)
        expr       = lake_reader(cfg.lake_root, cfg.year, cfg.month)
        top_df     = con.execute(f"""
            SELECT
                COALESCE(CAST({col_expr} AS VARCHAR), '(null)') AS value,
                COUNT(*) AS total
            FROM {expr}
            WHERE {col_expr} IS NOT NULL
              AND TRIM(CAST({col_expr} AS VARCHAR)) <> ''
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT {safe_top_n + 1}
        """).df()

        if top_df.empty:
            ws.write(0, 0, f"No data for {label}.", s.data)
            print(" ⚠️  no data")
            return

        truncated = len(top_df) > safe_top_n
        if truncated:
            top_df = top_df.iloc[:safe_top_n]
        top_vals = set(top_df["value"].tolist())
        n_vals   = len(top_vals)
        log.info(f"  {n_vals:,} distinct values" + (" (truncated)" if truncated else ""))

        matrix_data = matrix_counts_by_day(con, day_parts, col_expr, top_vals, label)
        if not matrix_data:
            ws.write(0, 0, f"No daily data for {label}.", s.data)
            print(" ⚠️  no daily data")
            return

        utc_dates = sorted(matrix_data.keys(), reverse=True)
        n_dates   = len(utc_dates)
        for c in range(1, n_dates + 1):
            ws.set_column(c, c, 12)

        row = 0
        _title_row(ws, row,
                   f"📅  {label} Matrix — top {n_vals:,} values × last {n_dates} days (IST)",
                   n_dates + 1, s)
        row += 1
        for c, h in enumerate([label] + [utc_date_str_to_ist_str(d) for d in utc_dates]):
            ws.write(row, c, h, s.hdr)
        row += 1

        data_start = row
        for idx, val in enumerate(top_df["value"]):
            is_alt = idx % 2 == 1
            d, n   = (s.alt, s.num_alt) if is_alt else (s.data, s.num)
            ws.write(row, 0, val, d)
            for c, utc_date in enumerate(utc_dates, 1):
                ws.write(row, c, matrix_data.get(utc_date, {}).get(val, 0), n)
            row += 1
            if (idx + 1) % 10_000 == 0:
                log.info(f"    {idx + 1:,} rows written …")

        ws.write(row, 0, "TOTAL", s.total_l)
        for c in range(1, n_dates + 1):
            col_l = xlsxwriter.utility.xl_col_to_name(c)
            ws.write(row, c, f"=SUM({col_l}{data_start+1}:{col_l}{row})", s.total)
        row += 1

        if truncated:
            warn = f"⚠️  Only top {safe_top_n:,} values shown (Excel row limit)."
            warn_fmt = wb.add_format({"bg_color": "#FFCCCC", "font_color": "#990000", "bold": True})
            for c in range(n_dates + 1):
                ws.write(row, c, warn if c == 0 else "", warn_fmt)

        ws.freeze_panes(2, 1)
        log.info(f"  Matrix '{label}' done ✓")
        print(" ✓")
    except Exception as e:
        print(f" ❌ Failed: {e}")
        log.error(f"Matrix {label} failed: {e}", exc_info=True)

def build_ip_matrix_with_state_channel(wb, con, cfg, cols, s):
    print("  🌐 Building Client IP matrix...", end="", flush=True)
    try:
        ip_col, state_col, qs_col = cfg.col_ip, cfg.col_state, cfg.col_qs
        if ip_col not in cols:
            log.warning("Client IP matrix skipped: 'cliIP' column missing.")
            print(" ⚠️  missing cliIP")
            return

        ws = wb.add_worksheet("Client IP_Matrix")
        ws.set_column(0, 0, 45)
        ws.set_column(1, 1, 20)
        ws.set_column(2, 2, 30)

        if cfg.col_ts not in cols:
            ws.write(0, 0, "reqTimeSec missing — cannot build matrix", s.data)
            print(" ⚠️  missing reqTimeSec")
            return

        day_parts  = discover_day_partitions(con, cfg.lake_root, cfg.year, cfg.month, cfg.matrix_days)
        if not day_parts:
            ws.write(0, 0, "No day partitions found.", s.data)
            print(" ⚠️  no partitions")
            return

        safe_top_n = min(cfg.matrix_top_n, 1_048_576 - 5)
        expr       = lake_reader(cfg.lake_root, cfg.year, cfg.month)
        top_ips_df = con.execute(f"""
            SELECT COALESCE(CAST({ip_col} AS VARCHAR), '(null)') AS ip, COUNT(*) AS total
            FROM {expr}
            WHERE {ip_col} IS NOT NULL AND TRIM(CAST({ip_col} AS VARCHAR)) <> ''
            GROUP BY 1 ORDER BY 2 DESC LIMIT {safe_top_n + 1}
        """).df()

        if top_ips_df.empty:
            ws.write(0, 0, "No IP data found.", s.data)
            print(" ⚠️  no IP data")
            return

        truncated = len(top_ips_df) > safe_top_n
        if truncated:
            top_ips_df = top_ips_df.iloc[:safe_top_n]
        top_ips = set(top_ips_df["ip"].tolist())
        log.info(f"  {len(top_ips):,} distinct IPs")

        ip_state: dict[str, str] = {}
        if state_col in cols:
            ms = con.execute(f"""
                WITH c AS (
                    SELECT COALESCE(CAST({ip_col} AS VARCHAR),'(null)') AS ip,
                           COALESCE(CAST({state_col} AS VARCHAR),'Unknown') AS state,
                           COUNT(*) AS cnt
                    FROM {expr}
                    WHERE {ip_col} IS NOT NULL AND TRIM(CAST({ip_col} AS VARCHAR)) <> ''
                    GROUP BY ip, state
                ),
                r AS (SELECT ip, state, ROW_NUMBER() OVER (PARTITION BY ip ORDER BY cnt DESC) AS rn FROM c)
                SELECT ip, state FROM r WHERE rn = 1
            """).df()
            if not ms.empty:
                ip_state = dict(zip(ms["ip"], ms["state"]))

        ip_channel: dict[str, str] = {}
        if qs_col in cols:
            mc = con.execute(f"""
                WITH c AS (
                    SELECT COALESCE(CAST({ip_col} AS VARCHAR),'(null)') AS ip,
                           COALESCE({channel_clean_expr(qs_col)},'Unknown') AS channel,
                           COUNT(*) AS cnt
                    FROM {expr}
                    WHERE {ip_col} IS NOT NULL AND TRIM(CAST({ip_col} AS VARCHAR)) <> ''
                      AND {qs_col} IS NOT NULL AND {qs_col} LIKE '%channel=%'
                    GROUP BY ip, channel
                ),
                r AS (SELECT ip, channel, ROW_NUMBER() OVER (PARTITION BY ip ORDER BY cnt DESC) AS rn FROM c)
                SELECT ip, channel FROM r WHERE rn = 1
            """).df()
            if not mc.empty:
                ip_channel = dict(zip(mc["ip"], mc["channel"]))

        matrix_data = matrix_counts_by_day(con, day_parts, ip_col, top_ips, "client_ip")
        if not matrix_data:
            ws.write(0, 0, "No daily data for IPs.", s.data)
            print(" ⚠️  no daily data")
            return

        utc_dates = sorted(matrix_data.keys(), reverse=True)
        n_dates   = len(utc_dates)
        for c in range(3, 3 + n_dates):
            ws.set_column(c, c, 12)

        row = 0
        _title_row(ws, row,
                   f"📅  Client IP Matrix — top {len(top_ips):,} IPs × last {n_dates} days (IST)",
                   3 + n_dates, s)
        row += 1
        for c, h in enumerate(
            ["Client IP", "State", "Channel"] + [utc_date_str_to_ist_str(d) for d in utc_dates]
        ):
            ws.write(row, c, h, s.hdr)
        row += 1

        data_start = row
        for idx, ip in enumerate(top_ips_df["ip"]):
            is_alt = idx % 2 == 1
            d, n   = (s.alt, s.num_alt) if is_alt else (s.data, s.num)
            ws.write(row, 0, ip,                              d)
            ws.write(row, 1, ip_state.get(ip, "N/A"),        d)
            ws.write(row, 2, ip_channel.get(ip, "N/A"),      d)
            for c, utc_date in enumerate(utc_dates, 3):
                ws.write(row, c, matrix_data.get(utc_date, {}).get(ip, 0), n)
            row += 1
            if (idx + 1) % 10_000 == 0:
                log.info(f"    {idx + 1:,} rows written …")

        ws.write(row, 0, "TOTAL", s.total_l)
        ws.write(row, 1, "", s.total_l)
        ws.write(row, 2, "", s.total_l)
        for c in range(3, 3 + n_dates):
            col_l = xlsxwriter.utility.xl_col_to_name(c)
            ws.write(row, c, f"=SUM({col_l}{data_start+1}:{col_l}{row})", s.total)
        row += 1

        if truncated:
            warn     = f"⚠️  Only top {safe_top_n:,} IPs shown (Excel row limit)."
            warn_fmt = wb.add_format({"bg_color": "#FFCCCC", "font_color": "#990000", "bold": True})
            for c in range(3 + n_dates):
                ws.write(row, c, warn if c == 0 else "", warn_fmt)

        ws.freeze_panes(2, 3)
        log.info("  Client IP matrix done ✓")
        print(" ✓")
    except Exception as e:
        print(f" ❌ Failed: {e}")
        log.error(f"IP matrix failed: {e}", exc_info=True)

def build_top_channels_by_state(wb, con, cfg, cols, s):
    print("  🗺️ Building Top Channels by State...", end="", flush=True)
    try:
        state_col, qs_col = cfg.col_state, cfg.col_qs
        if state_col not in cols or qs_col not in cols:
            log.warning("Top Channels by State skipped: missing 'state' or 'queryStr'.")
            print(" ⚠️  missing columns")
            return

        ws = wb.add_worksheet("Top Channels by State")
        ws.set_column(0, 0, 20)
        ws.set_column(1, 1, 40)
        ws.set_column(2, 2, 16)
        ws.set_column(3, 3, 10)

        expr = lake_reader(cfg.lake_root, cfg.year, cfg.month)
        df = con.execute(f"""
            WITH sct AS (
                SELECT
                    COALESCE(CAST({state_col} AS VARCHAR), 'Unknown') AS state,
                    COALESCE({channel_clean_expr(qs_col)}, 'Unknown') AS channel,
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
                FROM sct
            )
            SELECT state, channel, requests, rank
            FROM ranked WHERE rank <= 200
            ORDER BY state, rank
        """).df()

        if df.empty:
            ws.write(0, 0, "No data found.", s.data)
            print(" ⚠️  no data")
            return

        for c, h in enumerate(["State", "Channel", "Total Requests", "Rank"]):
            ws.write(0, c, h, s.hdr)

        for i, row_data in df.iterrows():
            is_alt = (i+1) % 2 == 0
            d, n   = (s.alt, s.num_alt) if is_alt else (s.data, s.num)
            ws.write(i+1, 0, row_data["state"],           d)
            ws.write(i+1, 1, row_data["channel"],         d)
            ws.write(i+1, 2, int(row_data["requests"]),   n)
            ws.write(i+1, 3, int(row_data["rank"]),       n)

        ws.freeze_panes(1, 0)
        log.info(f"  Top Channels by State done ✓ ({len(df)} rows)")
        print(" ✓")
    except Exception as e:
        print(f" ❌ Failed: {e}")
        log.error(f"Top Channels by State failed: {e}", exc_info=True)

def build_device_matrix_with_state(wb, con, cfg, cols, s):
    print("  📱 Building Device matrix...", end="", flush=True)
    try:
        device_col_expr = qs_extract(cfg.col_qs, "device")
        state_col, qs_col = cfg.col_state, cfg.col_qs

        if qs_col not in cols:
            log.warning("Device matrix skipped: queryStr column missing.")
            print(" ⚠️  missing queryStr")
            return
        if state_col not in cols:
            log.warning("Device matrix skipped: state column missing.")
            print(" ⚠️  missing state")
            return

        ws = wb.add_worksheet("Device_Matrix")
        ws.set_column(0, 0, 40)
        ws.set_column(1, 1, 20)
        for c in range(2, 2 + cfg.matrix_days):
            ws.set_column(c, c, 12)

        if cfg.col_ts not in cols:
            ws.write(0, 0, "reqTimeSec missing — cannot build matrix", s.data)
            print(" ⚠️  missing reqTimeSec")
            return

        day_parts = discover_day_partitions(con, cfg.lake_root, cfg.year, cfg.month, cfg.matrix_days)
        if not day_parts:
            ws.write(0, 0, "No day partitions found.", s.data)
            print(" ⚠️  no partitions")
            return

        safe_top_n   = min(cfg.matrix_top_n, 1_048_576 - 5)
        expr         = lake_reader(cfg.lake_root, cfg.year, cfg.month)
        top_devs_df  = con.execute(f"""
            SELECT COALESCE(CAST({device_col_expr} AS VARCHAR),'(null)') AS device, COUNT(*) AS total
            FROM {expr}
            WHERE {qs_col} IS NOT NULL AND {device_col_expr} IS NOT NULL
              AND TRIM(CAST({device_col_expr} AS VARCHAR)) <> ''
            GROUP BY 1 ORDER BY 2 DESC LIMIT {safe_top_n + 1}
        """).df()

        if top_devs_df.empty:
            ws.write(0, 0, "No device data found.", s.data)
            print(" ⚠️  no device data")
            return

        truncated = len(top_devs_df) > safe_top_n
        if truncated:
            top_devs_df = top_devs_df.iloc[:safe_top_n]
        top_devs = set(top_devs_df["device"].tolist())

        ms = con.execute(f"""
            WITH c AS (
                SELECT COALESCE(CAST({device_col_expr} AS VARCHAR),'(null)') AS device,
                       COALESCE(CAST({state_col} AS VARCHAR),'Unknown') AS state,
                       COUNT(*) AS cnt
                FROM {expr}
                WHERE {qs_col} IS NOT NULL AND {device_col_expr} IS NOT NULL
                  AND TRIM(CAST({device_col_expr} AS VARCHAR)) <> ''
                  AND {state_col} IS NOT NULL AND TRIM(CAST({state_col} AS VARCHAR)) <> ''
                GROUP BY device, state
            ),
            r AS (SELECT device, state, ROW_NUMBER() OVER (PARTITION BY device ORDER BY cnt DESC) AS rn FROM c)
            SELECT device, state FROM r WHERE rn = 1
        """).df()
        device_state_map = dict(zip(ms["device"], ms["state"])) if not ms.empty else {}

        matrix_data = matrix_counts_by_day(con, day_parts, device_col_expr, top_devs, "device")
        if not matrix_data:
            ws.write(0, 0, "No daily data for devices.", s.data)
            print(" ⚠️  no daily data")
            return

        utc_dates = sorted(matrix_data.keys(), reverse=True)
        n_dates   = len(utc_dates)

        row = 0
        _title_row(ws, row,
                   f"📅  Device Matrix — top {len(top_devs):,} devices × last {n_dates} days (IST)",
                   2 + n_dates, s)
        row += 1
        for c, h in enumerate(
            ["Device", "State"] + [utc_date_str_to_ist_str(d) for d in utc_dates]
        ):
            ws.write(row, c, h, s.hdr)
        row += 1

        data_start = row
        for idx, device in enumerate(top_devs_df["device"]):
            is_alt = idx % 2 == 1
            d, n   = (s.alt, s.num_alt) if is_alt else (s.data, s.num)
            ws.write(row, 0, device.replace("%20", " "),                          d)
            ws.write(row, 1, device_state_map.get(device, "N/A").replace("%20", " "), d)
            for c, utc_date in enumerate(utc_dates, 2):
                ws.write(row, c, matrix_data.get(utc_date, {}).get(device, 0), n)
            row += 1
            if (idx + 1) % 10_000 == 0:
                log.info(f"    {idx + 1:,} rows written …")

        ws.write(row, 0, "TOTAL", s.total_l)
        ws.write(row, 1, "", s.total_l)
        for c in range(2, 2 + n_dates):
            col_l = xlsxwriter.utility.xl_col_to_name(c)
            ws.write(row, c, f"=SUM({col_l}{data_start+1}:{col_l}{row})", s.total)
        row += 1

        if truncated:
            warn     = f"⚠️  Only top {safe_top_n:,} devices shown (Excel row limit)."
            warn_fmt = wb.add_format({"bg_color": "#FFCCCC", "font_color": "#990000", "bold": True})
            for c in range(2 + n_dates):
                ws.write(row, c, warn if c == 0 else "", warn_fmt)

        ws.freeze_panes(2, 2)
        log.info("  Device matrix done ✓")
        print(" ✓")
    except Exception as e:
        print(f" ❌ Failed: {e}")
        log.error(f"Device matrix failed: {e}", exc_info=True)

# ---- Main matrix report entry point ----
def run_matrix_report(s3_root: str) -> None:
    cfg = auto_config(s3_root)

    print(f"\n💾  Output path (Enter = {cfg.output_path}): ", end="")
    raw = input().strip()
    if raw:
        cfg.output_path = Path(raw).resolve()
    cfg.output_path.parent.mkdir(parents=True, exist_ok=True)
    if cfg.output_path.suffix.lower() != ".xlsx":
        cfg.output_path = cfg.output_path.with_suffix(".xlsx")

    cfg.year, cfg.month = prompt_year_month(s3_root)

    print("\n" + "═" * 58)
    print("  Matrix Report settings:")
    print(f"    Lake (S3): {cfg.lake_root}")
    print(f"    Output    : {cfg.output_path}")
    print(f"    Filter    : " +
          (f"year={cfg.year}, month={cfg.month}" if cfg.year else "none — full lake"))
    print(f"    Matrix    : top {cfg.matrix_top_n:,} values × {cfg.matrix_days} days")
    print("═" * 58)
    if input("\n  Proceed? (y/n): ").strip().lower() != "y":
        print("  Cancelled.")
        return

    print("\n🔌 Connecting to DuckDB and setting up S3...")
    con = None
    try:
        con = get_conn()
        print("  ✓ Connected, S3 secret configured")
    except Exception as e:
        print(f"  ❌ Failed to connect: {e}")
        return

    print("\n📋 Detecting schema (reading one row from S3)...")
    try:
        sample_reader = lake_reader(cfg.lake_root, cfg.year, cfg.month)
        sample_df = con.execute(f"SELECT * FROM {sample_reader} LIMIT 1").df()
        cols = set(sample_df.columns)
        print(f"  ✓ Found {len(cols)} columns: {', '.join(sorted(cols)[:10])}" + ("..." if len(cols)>10 else ""))
    except Exception as e:
        print(f"  ❌ Schema detection failed: {e}")
        con.close()
        return

    # Configure DuckDB
    con.execute(f"SET threads={cfg.threads};")
    con.execute(f"SET memory_limit='{cfg.memory_gb}GB';")
    con.execute("SET preserve_insertion_order=false;")
    td = Path(os.environ.get("TEMP", os.environ.get("TMP", "/tmp"))) / "duckdb_matrix_report"
    td.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory='{td.as_posix()}';")
    print(f"  ✓ DuckDB tuned: {cfg.threads} threads, {cfg.memory_gb}GB RAM, temp in {td}")

    print("\n📝 Creating Excel workbook...")
    try:
        wb = xlsxwriter.Workbook(str(cfg.output_path), {"constant_memory": True, "strings_to_urls": False})
        s = Styles(wb)
        print("  ✓ Workbook opened")
    except Exception as e:
        print(f"  ❌ Failed to create workbook: {e}")
        con.close()
        return

    # Build all sheets
    print("\n📊 Building report sheets:")
    build_overview(wb, con, cfg, cols, s)
    build_channel_platform(wb, con, cfg, cols, s)
    build_ip_matrix_with_state_channel(wb, con, cfg, cols, s)
    build_top_channels_by_state(wb, con, cfg, cols, s)

    direct_fields = [
        (cfg.col_ua,      "User Agent"),
        (cfg.col_asn,     "ASN"),
        (cfg.col_city,    "City"),
        (cfg.col_state,   "State"),
        (cfg.col_country, "Country"),
        (cfg.col_reqhost, "Request Host"),
    ]
    for col_name, label in direct_fields:
        if col_name in cols:
            build_matrix(wb, con, cfg, cols, s, col_name, label)
        else:
            print(f"  ⏭️  Skipping {label} matrix — column '{col_name}' not in lake")

    qs = cfg.col_qs
    if qs in cols:
        build_device_matrix_with_state(wb, con, cfg, cols, s)
        for col_expr, label in [
            (channel_clean_expr(qs),          "Channel"),
            (qs_extract(qs, "platform"),       "Platform"),
            (qs_extract(qs, "category_name"),  "Category"),
            (qs_extract(qs, "content_title"),  "Content Title"),
        ]:
            build_matrix(wb, con, cfg, cols, s, col_expr, label)
    else:
        print(f"  ⏭️  Skipping queryString matrices — column '{qs}' not in lake")

    print("\n💾 Saving Excel file...")
    try:
        wb.close()
        print(f"  ✅ Matrix report saved: {cfg.output_path}")
    except Exception as e:
        print(f"  ❌ Failed to save: {e}")
    finally:
        con.close()

    print(f"\n{'═' * 58}")
    print(f"  ✅  Matrix report generation finished.")
    print(f"{'═' * 58}\n")

# ──────────────────────────────────────────────────────────────
# DETAILED ANALYSIS (adapted for S3)
# ──────────────────────────────────────────────────────────────
def run_detailed_analysis(s3_root: str) -> None:
    print("\n" + "═" * 58)
    print("  Detailed Analysis  –  append-aware (S3 source)")
    print("═" * 58 + "\n")
    print("💾  Output folder (Excel will be saved / updated here):")
    out_dir = prompt_path("  Output folder path: ", must_be_dir=True)
    output_file = out_dir / DETAILED_ANALYSIS_FILENAME
    is_first_run = not output_file.exists()
    existing_dates = get_existing_dates(output_file, DETAILED_SHEET_NAME) if not is_first_run else set()
    if existing_dates:
        print(f"\n  📋  Existing file with {len(existing_dates)} date(s).")
    year_filter, month_filter = prompt_year_month(s3_root)
    print("\n" + "─" * 58)
    print(f"  Lake (S3)   : {s3_root}")
    print(f"  Output file : {output_file}")
    print(f"  Mode        : {'CREATE' if is_first_run else 'APPEND'}")
    print("─" * 58)
    if input("\n  Proceed? (y/n): ").strip().lower() != "y":
        print("  Cancelled.")
        return

    print("\n🔌 Connecting to DuckDB...")
    con = get_conn()
    print("  ✓ Connected")

    reader = lake_reader(s3_root, year_filter, month_filter)
    qs_col, ua_col = "queryStr", "UA"
    sql = f"""
        SELECT
            make_date(CAST(year AS INT), CAST(month AS INT), CAST(day AS INT)) AS utc_date,
            state,
            {ua_col},
            {channel_clean_expr(qs_col)} AS channel,
            COALESCE({qs_extract(qs_col, 'platform')}, 'Unknown') AS platform,
            COALESCE({qs_extract(qs_col, 'device')}, 'Unknown') AS device,
            COUNT(*) AS requests,
            COUNT(DISTINCT {qs_extract(qs_col, 'device_id')}) AS unique_devices,
            COUNT(DISTINCT {qs_extract(qs_col, 'session_id')}) AS unique_sessions
        FROM {reader}
        WHERE state IS NOT NULL AND state != ''
          AND {qs_col} IS NOT NULL AND {qs_col} LIKE '%channel=%'
        GROUP BY year, month, day, state, {ua_col}, channel, platform, device
        ORDER BY utc_date
    """
    print("\n🔄 Executing query (this may take a while)...")
    try:
        start = time.time()
        df = con.execute(sql).df()
        elapsed = time.time() - start
        print(f"  ✓ Query completed in {elapsed:.1f} seconds, {len(df):,} rows returned.")
    except Exception as e:
        print(f"  ❌ SQL error: {e}")
        con.close()
        return
    con.close()

    if df.empty:
        print("  No data found for the selected filters.")
        return

    df["ist_date"] = pd.to_datetime(df["utc_date"]) + IST_OFFSET
    df["Date (IST)"] = df["ist_date"].dt.strftime("%Y-%m-%d")
    if existing_dates:
        before = len(df)
        df = df[~df["Date (IST)"].isin(existing_dates)]
        print(f"  ℹ️  Skipped {before - len(df)} rows for already-loaded dates.")
    if df.empty:
        print("  ✅ Nothing new to add — your file is already up to date.")
        return

    total_per_day = df.groupby("Date (IST)")["requests"].transform("sum")
    df["% of Requests"] = (df["requests"] / total_per_day * 100).round(2)
    output_df = df[[
        "Date (IST)", "state", ua_col, "channel", "platform", "device",
        "requests", "unique_devices", "unique_sessions", "% of Requests"
    ]].copy()
    output_df.columns = [
        "Date (IST)", "State", "User Agent", "Channel", "Platform", "Device",
        "Requests", "Unique Devices", "Unique Sessions", "% of Requests"
    ]
    output_df = clean_percent_encoding(output_df)
    output_df["Device"] = output_df["Device"].apply(decode_device)

    print("\n💾 Writing Excel file...")
    if is_first_run:
        write_to_excel(output_file, output_df, sheet_name=DETAILED_SHEET_NAME)
        print(f"  ✅ Created: {output_file}")
    else:
        merge_and_rewrite(output_file, output_df, sheet_name=DETAILED_SHEET_NAME, date_column="Date (IST)")
        print(f"  ✅ Updated: {output_file}")

    print("\n  💡 Tip: Open in Excel → Insert Pivot Table → slicers on State, Channel, Platform, Device.")

# ──────────────────────────────────────────────────────────────
# MAIN MENU
# ──────────────────────────────────────────────────────────────
def main() -> None:
    print("\n" + "═" * 58)
    print("  Lake Report Generator (Linode S3)")
    print("═" * 58)
    s3_root = LAKE_S3_ROOT
    print(f"\n🌎  Using S3 lake: {s3_root}")
    print("\n  What would you like to generate?\n")
    print("    1 · Matrix Report")
    print("    2 · Detailed Analysis")
    print("    3 · Both reports")
    choice = input("  Enter 1 / 2 / 3: ").strip()
    if choice == "1":
        run_matrix_report(s3_root)
    elif choice == "2":
        run_detailed_analysis(s3_root)
    elif choice == "3":
        run_matrix_report(s3_root)
        run_detailed_analysis(s3_root)
    else:
        print("  Invalid choice.")
    print("\n  All done.\n")

if __name__ == "__main__":
    main()