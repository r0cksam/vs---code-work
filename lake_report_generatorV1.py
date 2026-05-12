#!/usr/bin/env python3
"""
lake_report_final.py
═══════════════════════════════════════════════════════════════════════
Generates Excel report with matrices (top values × recent dates).
- Overview: metadata + daily breakdown (rows, distinct IPs, session/device presence)
- Channel × Platform pivot
- Matrix sheets for: UA, ASN, City, State, Country, reqHost,
  Channel, Platform, Device, Category, Content (all from queryString)
"""

import sys
import re
from pathlib import Path
from datetime import datetime, timezone

import duckdb
import pandas as pd
import xlsxwriter

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════

LAKE_FOLDER   = Path(r"Z:\05 Veto Logs\lake")   # override via CLI
OUTPUT_FOLDER = LAKE_FOLDER.parent
THREADS       = 8
MEMORY        = "16GB"
MATRIX_TOP_N  = 100000      # rows per matrix
MATRIX_MAX_DAYS = 60        # recent date columns

FIELD_MAP = {
    "ua":       "UA",
    "asn":      "asn",
    "city":     "city",
    "state":    "state",
    "country":  "country",
    "reqhost":  "reqHost",
    "qs":       "queryStr",
    "ip":       "cliIP",
    "ts":       "reqTimeSec",
}

# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════

def sanitize_sheet_name(name: str) -> str:
    """Replace Excel‑illegal characters with underscore."""
    illegal = r'[]:*?/\\'
    return re.sub(f'[{re.escape(illegal)}]', '_', name)[:31]

def get_conn(lake_root: Path):
    con = duckdb.connect()
    con.execute(f"SET threads={THREADS};")
    con.execute(f"SET memory_limit='{MEMORY}';")
    con.execute("SET preserve_insertion_order=false;")
    return con

def lake_reader(lake_root: Path) -> str:
    return f"read_parquet('{lake_root.as_posix()}/**/*.parquet', hive_partitioning=true, union_by_name=true)"

def detect_columns(con, lake_root: Path) -> set:
    try:
        df = con.execute(f"DESCRIBE SELECT * FROM {lake_reader(lake_root)} LIMIT 0").df()
        return set(df["column_name"].tolist())
    except Exception:
        return set()

def qs_extract(qs_col: str, param: str) -> str:
    return f"NULLIF(regexp_extract({qs_col}, '(?:^|&){param}=([^&]+)', 1), '')"

def safe_df(con, sql: str, label: str = "") -> pd.DataFrame:
    try:
        return con.execute(sql).df()
    except Exception as e:
        print(f"  ⚠️  Query failed{' (' + label + ')' if label else ''}: {e}")
        return pd.DataFrame()

# ═══════════════════════════════════════════════════════════════
# XLSXWRITER STYLES
# ═══════════════════════════════════════════════════════════════

def _hdr_fmt(wb, bg="#2E75B6", fg="#FFFFFF"):
    return wb.add_format({'font_name':'Arial','font_size':10,'bold':True,
                          'font_color':fg,'bg_color':bg,'align':'center',
                          'valign':'vcenter','border':1,'border_color':'#CCCCCC',
                          'text_wrap':True})

def _data_fmt(wb, bg="#FFFFFF", align="left"):
    return wb.add_format({'font_name':'Arial','font_size':10,'font_color':'#000000',
                          'bg_color':bg,'border':1,'border_color':'#CCCCCC',
                          'align':align,'valign':'vcenter'})

def _title_fmt(wb):
    return wb.add_format({'font_name':'Arial','font_size':11,'bold':True,
                          'font_color':'#FFFFFF','bg_color':'#1F3864',
                          'align':'left','valign':'vcenter'})

def _total_fmt(wb):
    return wb.add_format({'font_name':'Arial','font_size':10,'bold':True,
                          'font_color':'#FFFFFF','bg_color':'#E36209',
                          'border':1,'border_color':'#CCCCCC','align':'right'})

def _total_label_fmt(wb):
    f = _total_fmt(wb)
    f.set_align('left')
    return f

# ═══════════════════════════════════════════════════════════════
# OVERVIEW SHEET (simplified)
# ═══════════════════════════════════════════════════════════════

def build_overview(workbook, con, lake_root, existing, generated_at):
    ws = workbook.add_worksheet("Overview")
    ws.set_column("A:A", 28)
    ws.set_column("B:B", 36)

    title = _title_fmt(workbook)
    kv_key = _data_fmt(workbook, bg="#D6E4F0", align="left")
    kv_val = _data_fmt(workbook, bg="#FFFFFF", align="left")
    hdr = _hdr_fmt(workbook)
    data = _data_fmt(workbook)
    data_alt = _data_fmt(workbook, bg="#F2F2F2")
    total_f = _total_fmt(workbook)
    total_lab = _total_label_fmt(workbook)

    row = 0
    ws.merge_range(row, 0, row, 1, "📊  Report Metadata", title)
    row += 1
    ws.write(row, 0, "Report generated", kv_key)
    ws.write(row, 1, generated_at, kv_val)
    row += 1

    reader = lake_reader(lake_root)
    ts_col = FIELD_MAP["ts"]
    if ts_col in existing:
        meta = safe_df(con, f"""
            SELECT
                MIN(to_timestamp(CAST({ts_col} AS DOUBLE)))::DATE AS min_date,
                MAX(to_timestamp(CAST({ts_col} AS DOUBLE)))::DATE AS max_date,
                MIN(to_timestamp(CAST({ts_col} AS DOUBLE)))       AS min_ts,
                MAX(to_timestamp(CAST({ts_col} AS DOUBLE)))       AS max_ts,
                COUNT(*) AS total_rows
            FROM {reader}
        """, "global meta")
        if not meta.empty:
            r = meta.iloc[0]
            ws.write(row, 0, "Data date range", kv_key)
            ws.write(row, 1, f"{r['min_date']} → {r['max_date']}", kv_val)
            row += 1
            ws.write(row, 0, "Data time range", kv_key)
            ws.write(row, 1, f"{str(r['min_ts'])[:19]} → {str(r['max_ts'])[:19]}", kv_val)
            row += 1
            ws.write(row, 0, "Total rows (all time)", kv_key)
            ws.write(row, 1, f"{int(r['total_rows']):,}", kv_val)
            row += 2
    else:
        ws.write(row, 0, "Data date/time range", kv_key)
        ws.write(row, 1, "reqTimeSec column not found", kv_val)
        row += 2

    # Day-by-day breakdown (custom columns)
    ws.merge_range(row, 0, row, 9, "📅  Day-by-Day Breakdown", title)
    row += 1
    headers = [
        "Date", "Total Rows", "cliIP rows", "Distinct cliIP",
        "Rows with session_id", "Distinct session_id",
        "Rows with device_id", "Distinct device_id",
        "Rows where device_id is blank", "Rows where session_id is blank"
    ]
    for col, h in enumerate(headers):
        ws.write(row, col, h, hdr)
    row += 1

    if ts_col in existing:
        qs = FIELD_MAP["qs"]
        has_qs = qs in existing
        ip_col = FIELD_MAP["ip"]
        has_ip = ip_col in existing

        ip_rows = f"COUNT({ip_col})" if has_ip else "0"
        distinct_ip = f"COUNT(DISTINCT {ip_col})" if has_ip else "0"

        if has_qs:
            # Rows containing the parameter (any value, including empty)
            sess_present = f"SUM(CASE WHEN {qs} LIKE '%session_id=%' THEN 1 ELSE 0 END)"
            dev_present = f"SUM(CASE WHEN {qs} LIKE '%device_id=%' THEN 1 ELSE 0 END)"

            # Distinct non‑empty values
            sess_distinct = f"COUNT(DISTINCT CASE WHEN {qs_extract(qs, 'session_id')} IS NOT NULL AND {qs_extract(qs, 'session_id')} != '' THEN {qs_extract(qs, 'session_id')} END)"
            dev_distinct = f"COUNT(DISTINCT CASE WHEN {qs_extract(qs, 'device_id')} IS NOT NULL AND {qs_extract(qs, 'device_id')} != '' THEN {qs_extract(qs, 'device_id')} END)"

            # Blank rows: parameter exists but extracted value is NULL or empty string
            dev_blank = f"SUM(CASE WHEN {qs} LIKE '%device_id=%' AND ( {qs_extract(qs, 'device_id')} IS NULL OR {qs_extract(qs, 'device_id')} = '' ) THEN 1 ELSE 0 END)"
            sess_blank = f"SUM(CASE WHEN {qs} LIKE '%session_id=%' AND ( {qs_extract(qs, 'session_id')} IS NULL OR {qs_extract(qs, 'session_id')} = '' ) THEN 1 ELSE 0 END)"
        else:
            sess_present = sess_distinct = dev_present = dev_distinct = dev_blank = sess_blank = "0"

        day_sql = f"""
            SELECT
                make_date(CAST(year AS INT), CAST(month AS INT), CAST(day AS INT)) AS date,
                COUNT(*) AS total_rows,
                {ip_rows} AS ip_rows,
                {distinct_ip} AS distinct_ip,
                {sess_present} AS sess_rows,
                {sess_distinct} AS distinct_sess,
                {dev_present} AS dev_rows,
                {dev_distinct} AS distinct_dev,
                {dev_blank} AS dev_blank,
                {sess_blank} AS sess_blank
            FROM {reader}
            GROUP BY year, month, day
            ORDER BY year, month, day
        """
        day_df = safe_df(con, day_sql, "day breakdown")
        for _, r in day_df.iterrows():
            dt = r["date"]
            date_str = dt.strftime("%d/%m/%y") if hasattr(dt, "strftime") else str(dt)
            vals = [
                date_str,
                int(r["total_rows"]),
                int(r["ip_rows"]),
                int(r["distinct_ip"]),
                int(r["sess_rows"]),
                int(r["distinct_sess"]),
                int(r["dev_rows"]),
                int(r["distinct_dev"]),
                int(r["dev_blank"]),
                int(r["sess_blank"]),
            ]
            for col, v in enumerate(vals):
                fmt = data_alt if row % 2 == 0 else data
                ws.write(row, col, v, fmt)
            row += 1

        # Total row
        if len(day_df) > 0:
            start_row = row - len(day_df)
            total_vals = ["TOTAL"] + [f"=SUM({chr(65+c)}{start_row+1}:{chr(65+c)}{row})" for c in range(1, 10)]
            for col, v in enumerate(total_vals):
                fmt = total_lab if col == 0 else total_f
                ws.write(row, col, v, fmt)
            row += 1

    ws.freeze_panes(2, 1)
    
# ═══════════════════════════════════════════════════════════════
# CHANNEL × PLATFORM (static pivot)
# ═══════════════════════════════════════════════════════════════

def build_channel_platform(workbook, con, lake_root, existing):
    ws = workbook.add_worksheet("Channel×Platform")
    ws.set_column("A:A", 40)
    ws.set_column("B:B", 20)
    ws.set_column("C:C", 16)
    ws.set_column("D:D", 16)
    ws.set_column("E:E", 16)

    title = _title_fmt(workbook)
    hdr = _hdr_fmt(workbook)
    data = _data_fmt(workbook)
    data_alt = _data_fmt(workbook, bg="#F2F2F2")

    reader = lake_reader(lake_root)
    qs = FIELD_MAP["qs"]
    if qs not in existing:
        ws.write(0, 0, "queryStr column not found.", data)
        return

    row = 0
    ws.merge_range(row, 0, row, 4, "📡  Channel × Platform breakdown", title)
    row += 1
    headers = ["Channel", "Platform", "Requests", "Unique Devices", "Unique Sessions"]
    for col, h in enumerate(headers):
        ws.write(row, col, h, hdr)
    row += 1

    sql = f"""
        SELECT
            COALESCE({qs_extract(qs,'channel')}, {qs_extract(qs,'channel_name')}, 'Unknown') AS channel,
            COALESCE({qs_extract(qs,'platform')}, 'Unknown') AS platform,
            COUNT(*) AS requests,
            COUNT(DISTINCT {qs_extract(qs,'device_id')}) AS devices,
            COUNT(DISTINCT {qs_extract(qs,'session_id')}) AS sessions
        FROM {reader}
        WHERE {qs} IS NOT NULL AND {qs} LIKE '%channel=%'
        GROUP BY 1, 2
        ORDER BY 3 DESC
        LIMIT 5000
    """
    df = safe_df(con, sql, "channel×platform")
    if df.empty:
        ws.write(row, 0, "No channel data found.", data)
        return

    for _, r in df.iterrows():
        fmt = data_alt if row % 2 == 0 else data
        ws.write(row, 0, r["channel"], fmt)
        ws.write(row, 1, r["platform"], fmt)
        ws.write(row, 2, r["requests"], fmt)
        ws.write(row, 3, r["devices"], fmt)
        ws.write(row, 4, r["sessions"], fmt)
        row += 1

    ws.freeze_panes(2, 0)

# ═══════════════════════════════════════════════════════════════
# GENERIC MATRIX SHEET (top values × recent dates)
# ═══════════════════════════════════════════════════════════════

def build_matrix_sheet(workbook, con, lake_root, col_expr, field_name, existing,
                       top_n=MATRIX_TOP_N, max_days=MATRIX_MAX_DAYS):
    sheet_name = sanitize_sheet_name(f"{field_name}_Matrix")
    ws = workbook.add_worksheet(sheet_name)
    ws.set_column(0, 0, 50)
    for i in range(1, max_days + 1):
        ws.set_column(i, i, 14)

    title_fmt = _title_fmt(workbook)
    hdr_fmt = _hdr_fmt(workbook)
    data_fmt = _data_fmt(workbook)
    data_alt = _data_fmt(workbook, bg="#F2F2F2")
    total_f = _total_fmt(workbook)
    total_lab = _total_label_fmt(workbook)

    reader = lake_reader(lake_root)
    ts = FIELD_MAP["ts"]
    if ts not in existing:
        ws.write(0, 0, f"reqTimeSec missing – cannot build matrix for {field_name}", data_fmt)
        return

    print(f"    → Fetching top {top_n:,} values for {field_name}...")
    top_sql = f"""
        SELECT
            COALESCE(CAST({col_expr} AS VARCHAR), '(null)') AS value,
            COUNT(*) AS total
        FROM {reader}
        WHERE {col_expr} IS NOT NULL AND TRIM(CAST({col_expr} AS VARCHAR)) <> ''
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT {top_n}
    """
    top_df = safe_df(con, top_sql, f"top_{field_name}")
    if top_df.empty:
        ws.write(0, 0, f"No data for {field_name}", data_fmt)
        return

    # Temporary table with top values
    con.execute("CREATE TEMP TABLE top_values (value VARCHAR PRIMARY KEY)")
    con.executemany("INSERT INTO top_values VALUES (?)", top_df[["value"]].values.tolist())

    print(f"    → Daily counts for {len(top_df)} values, last {max_days} days...")
    daily_sql = f"""
        WITH last_days AS (
            SELECT DISTINCT
                make_date(CAST(year AS INT), CAST(month AS INT), CAST(day AS INT)) AS date
            FROM {reader}
            WHERE {ts} IS NOT NULL
            ORDER BY date DESC
            LIMIT {max_days}
        ),
        daily_counts AS (
            SELECT
                d.date,
                COALESCE(CAST(src.{col_expr} AS VARCHAR), '(null)') AS value,
                COUNT(*) AS cnt
            FROM {reader} src
            JOIN last_days d ON make_date(CAST(src.year AS INT), CAST(src.month AS INT), CAST(src.day AS INT)) = d.date
            WHERE src.{col_expr} IS NOT NULL
              AND TRIM(CAST(src.{col_expr} AS VARCHAR)) <> ''
              AND COALESCE(CAST(src.{col_expr} AS VARCHAR), '(null)') IN (SELECT value FROM top_values)
            GROUP BY d.date, src.{col_expr}
        )
        SELECT date, value, cnt FROM daily_counts
        ORDER BY date DESC, cnt DESC
    """
    daily_df = safe_df(con, daily_sql, f"matrix_{field_name}")
    con.execute("DROP TABLE top_values")

    if daily_df.empty:
        ws.write(0, 0, f"No daily data for {field_name}", data_fmt)
        return

    print(f"    → Pivoting...")
    pivot = daily_df.pivot(index="value", columns="date", values="cnt").fillna(0).astype(int)
    dates = sorted(pivot.columns, reverse=True)
    pivot = pivot[dates]

    row = 0
    ws.merge_range(row, 0, row, len(dates), f"📅  {field_name} Matrix — Top {len(pivot):,} values x last {len(dates)} days", title_fmt)
    row += 1
    headers = [field_name] + [d.strftime("%d/%m/%y") for d in dates]
    for col, h in enumerate(headers):
        ws.write(row, col, h, hdr_fmt)
    row += 1

    start_row = row
    print(f"    → Writing {len(pivot)} rows...")
    for idx, (val, row_data) in enumerate(pivot.iterrows()):
        fmt = data_alt if (row - start_row) % 2 == 0 else data_fmt
        ws.write(row, 0, val, fmt)
        for col, date in enumerate(dates, 1):
            ws.write(row, col, row_data[date], fmt)
        row += 1
        if (idx+1) % 20000 == 0:
            print(f"        Written {idx+1} rows")

    # Total row
    ws.write(row, 0, "TOTAL", total_lab)
    for col in range(1, len(dates)+1):
        col_letter = xlsxwriter.utility.xl_col_to_name(col)
        ws.write(row, col, f"=SUM({col_letter}{start_row+1}:{col_letter}{row})", total_f)

    ws.freeze_panes(2, 1)
    print(f"    → Matrix '{field_name}' done.")

# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    lake_root = Path(sys.argv[1]) if len(sys.argv) > 1 else LAKE_FOLDER
    lake_root = lake_root.resolve()
    if not lake_root.exists():
        print(f"❌  Lake folder not found: {lake_root}")
        sys.exit(1)

    print(f"\n{'═'*60}")
    print(f"  Lake Report Generator (FINAL – matrices only)")
    print(f"  Lake   : {lake_root}")
    print(f"  Matrix rows : {MATRIX_TOP_N:,}  |  Days : {MATRIX_MAX_DAYS}")
    print(f"{'═'*60}\n")

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    out_path = OUTPUT_FOLDER / f"lake_report_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"

    print("  Connecting to DuckDB …")
    con = get_conn(lake_root)
    print("  Detecting columns …")
    existing = detect_columns(con, lake_root)
    print(f"  Found {len(existing)} columns.\n")

    print("  Creating Excel workbook (XlsxWriter) …")
    workbook = xlsxwriter.Workbook(str(out_path), {'constant_memory': True})

    # 1. Overview (simplified)
    print("  Building Overview …")
    build_overview(workbook, con, lake_root, existing, generated_at)

    # 2. Channel×Platform
    print("  Building Channel×Platform …")
    build_channel_platform(workbook, con, lake_root, existing)

    # 3. Matrix sheets for direct columns
    direct_fields = [
        ("UA", FIELD_MAP["ua"], "User Agent"),
        ("ASN", FIELD_MAP["asn"], "ASN"),
        ("City", FIELD_MAP["city"], "City"),
        ("State", FIELD_MAP["state"], "State"),
        ("Country", FIELD_MAP["country"], "Country"),
        ("reqHost", FIELD_MAP["reqhost"], "Request Host"),
    ]
    for sheet_name, col, label in direct_fields:
        if col in existing:
            print(f"  Matrix: {sheet_name} …")
            build_matrix_sheet(workbook, con, lake_root, col, label, existing)
        else:
            print(f"  Skipping {sheet_name}: column missing")

    # 4. Matrix sheets for queryString-derived fields
    qs = FIELD_MAP["qs"]
    if qs in existing:
        qs_fields = [
            ("Channel", qs_extract(qs, "channel"), "Channel"),
            ("Platform", qs_extract(qs, "platform"), "Platform"),
            ("Device", qs_extract(qs, "device"), "Device"),
            ("Category", qs_extract(qs, "category_name"), "Category"),
            ("Content", qs_extract(qs, "content_title"), "Content"),
        ]
        for sheet_name, expr, label in qs_fields:
            print(f"  Matrix: {sheet_name} …")
            build_matrix_sheet(workbook, con, lake_root, expr, label, existing)
    else:
        print("  Skipping queryString matrices – column missing")

    print(f"\n  Saving → {out_path}")
    workbook.close()
    con.close()

    print(f"\n{'═'*60}")
    print(f"  ✅  Report saved: {out_path}")
    print(f"{'═'*60}\n")

if __name__ == "__main__":
    main()