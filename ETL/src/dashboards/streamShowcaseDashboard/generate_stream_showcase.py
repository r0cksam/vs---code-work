#!/usr/bin/env python3
"""Generate a stakeholder-friendly STREAM watch-hours showcase.

The dashboard reads the existing watch-hours marts, not the raw lake.  That keeps
the PR showcase fast while using the same validated data as Veto Watch Hours.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime
import json
import os
from pathlib import Path
import tempfile
from typing import Any

import pandas as pd


HERE = Path(__file__).resolve().parent
ETL_ROOT = HERE.parents[2]
DEFAULT_WATCH_ROOT = ETL_ROOT / "output" / "watch_hours"
DEFAULT_OUT_DIR = DEFAULT_WATCH_ROOT / "stream_showcase"
DEFAULT_CHARTJS_CACHE = ETL_ROOT / "output" / "cache" / "chartjs" / "chart.umd.min.js"

CHUNK_HOURS = 6 / 3600


def read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Required parquet not found: {path}")
    return pd.read_parquet(path)


def to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0)


def as_date_text(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m-%d")


def clean_text(series: pd.Series, fallback: str = "Unknown / NA") -> pd.Series:
    out = series.fillna("").astype(str).str.strip()
    return out.where(out.ne(""), fallback)


def atomic_write_text(path: Path, text: str, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path: Path | None = None
    try:
        fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
        os.close(fd)
        tmp_path = Path(tmp)
        tmp_path.write_text(text, encoding=encoding)
        tmp_path.replace(path)
    finally:
        if tmp_path is not None and tmp_path.exists():
            tmp_path.unlink()


def json_blob(data: dict[str, Any]) -> str:
    return json.dumps(data, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")


def load_chartjs(cache_path: Path) -> str:
    if cache_path.exists():
        return cache_path.read_text(encoding="utf-8")
    return "window.Chart=null;"


def completed_window(daily: pd.DataFrame) -> tuple[str, str]:
    stream_days = daily[daily["source"].eq("stream")].copy()
    stream_days = stream_days[to_numeric(stream_days["raw_ts_rows"]).gt(0)]
    if stream_days.empty:
        raise ValueError("No STREAM watch-hour rows found in daily_volume.parquet")

    today_text = date.today().strftime("%Y-%m-%d")
    complete = stream_days[stream_days["log_date"].lt(today_text)].copy()
    if complete.empty:
        complete = stream_days
    return str(complete["log_date"].min()), str(complete["log_date"].max())


def trailing_window(daily: pd.DataFrame, days: int) -> tuple[str, str]:
    ordered = daily.sort_values("log_date").reset_index(drop=True)
    if ordered.empty:
        raise ValueError("Cannot choose a trailing showcase range from empty daily data")
    window = ordered.tail(days)
    return str(window["log_date"].min()), str(window["log_date"].max())


def add_watch_hours(df: pd.DataFrame, row_col: str = "raw_ts_rows") -> pd.DataFrame:
    out = df.copy()
    if "raw_watch_hours" not in out.columns and row_col in out.columns:
        out["raw_watch_hours"] = to_numeric(out[row_col]) * CHUNK_HOURS
    return out


def aggregate_daily(daily: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    out = daily[
        daily["source"].eq("stream")
        & daily["log_date"].between(start, end)
    ].copy()
    out["raw_watch_hours"] = to_numeric(out["raw_ts_rows"]) * CHUNK_HOURS
    keep = [
        "log_date",
        "raw_watch_hours",
        "raw_ts_rows",
        "m3u8_rows",
        "rows",
        "approx_unique_ips",
    ]
    return out[keep].sort_values("log_date")


def group_sum(df: pd.DataFrame, keys: list[str], value_cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in value_cols:
        if col in out.columns:
            out[col] = to_numeric(out[col])
    present = [col for col in value_cols if col in out.columns]
    return out.groupby(keys, dropna=False)[present].sum().reset_index()


def best_range_hints(daily: pd.DataFrame) -> list[dict[str, Any]]:
    ordered = daily.sort_values("log_date").reset_index(drop=True).copy()
    hints: list[dict[str, Any]] = []

    def append_hint(label: str, start: str, end: str, days: int, hours: float, note: str) -> None:
        hints.append(
            {
                "label": label,
                "start": start,
                "end": end,
                "days": int(days),
                "rawHours": round(float(hours), 3),
                "note": note,
            }
        )

    total_hours = float(ordered["raw_watch_hours"].sum())
    append_hint(
        "Max total",
        str(ordered["log_date"].min()),
        str(ordered["log_date"].max()),
        int(len(ordered)),
        total_hours,
        "Largest cumulative number because it uses the full completed range.",
    )
    if len(ordered) >= 32:
        latest_32 = ordered.tail(32)
        append_hint(
            "Latest 32D",
            str(latest_32["log_date"].min()),
            str(latest_32["log_date"].max()),
            32,
            float(latest_32["raw_watch_hours"].sum()),
            "Default PR showcase range.",
        )

    for window in [1, 7, 15, 30]:
        if len(ordered) < window:
            continue
        roll = ordered["raw_watch_hours"].rolling(window=window).sum()
        idx = int(roll.idxmax())
        start_idx = idx - window + 1
        append_hint(
            f"Best {window}D" if window > 1 else "Best day",
            str(ordered.loc[start_idx, "log_date"]),
            str(ordered.loc[idx, "log_date"]),
            window,
            float(roll.loc[idx]),
            f"Highest continuous {window}-day STREAM watch-hours window.",
        )
    return hints


def records(df: pd.DataFrame, rename: dict[str, str] | None = None, limit: int | None = None) -> list[dict[str, Any]]:
    out = df.rename(columns=rename or {}).copy()
    if limit is not None:
        out = out.head(limit)
    for col in out.columns:
        if pd.api.types.is_float_dtype(out[col]):
            out[col] = out[col].round(3)
    return out.to_dict(orient="records")


def build_data(watch_root: Path, title: str) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    daily = read_parquet(watch_root / "daily_tables" / "daily_volume.parquet")
    geo = read_parquet(watch_root / "daily_tables" / "geo_daily.parquet")

    for frame in [daily, geo]:
        frame["log_date"] = as_date_text(frame["log_date"])

    start, end = completed_window(daily)
    daily_stream = aggregate_daily(daily, start, end)
    default_start, default_end = trailing_window(daily_stream, 32)

    geo = add_watch_hours(geo)
    geo = geo[geo["source"].eq("stream") & geo["log_date"].between(start, end)].copy()
    geo["country"] = clean_text(geo["country"])
    geo["state"] = clean_text(geo["state"])
    geo["city"] = clean_text(geo["city"])

    country_summary = group_sum(
        geo,
        ["country"],
        ["raw_watch_hours", "approx_unique_ips"],
    ).sort_values("raw_watch_hours", ascending=False)
    state_summary = group_sum(
        geo[geo["country"].eq("IN")],
        ["state"],
        ["raw_watch_hours", "approx_unique_ips"],
    ).sort_values("raw_watch_hours", ascending=False)
    state_summary["avg_min_per_user_day"] = (
        state_summary["raw_watch_hours"] * 60 / state_summary["approx_unique_ips"].replace(0, pd.NA)
    ).fillna(0)

    default_daily = daily_stream[daily_stream["log_date"].between(default_start, default_end)].copy()
    default_geo = geo[geo["log_date"].between(default_start, default_end)].copy()
    total_hours = float(default_daily["raw_watch_hours"].sum())
    user_days = float(to_numeric(default_daily["approx_unique_ips"]).sum())
    day_count = int(len(default_daily))
    india_hours = float(default_geo.loc[default_geo["country"].eq("IN"), "raw_watch_hours"].sum())

    summary = {
        "rawHours": round(total_hours, 3),
        "avgDailyUsers": round(user_days / day_count, 1) if day_count else 0,
        "peakDailyUsers": int(to_numeric(daily_stream["approx_unique_ips"]).max()),
        "peakDailyUserDate": str(daily_stream.loc[to_numeric(daily_stream["approx_unique_ips"]).idxmax(), "log_date"]),
        "avgMinPerUserDay": round(total_hours * 60 / user_days, 2) if user_days else 0,
        "indiaHours": round(india_hours, 3),
        "indiaShare": round(india_hours * 100 / total_hours, 2) if total_hours else 0,
    }

    daily_records = records(
        daily_stream,
        {
            "log_date": "date",
            "raw_watch_hours": "rawHours",
            "approx_unique_ips": "userDays",
        },
    )

    state_daily = group_sum(
        geo[geo["country"].eq("IN")],
        ["log_date", "state"],
        ["raw_watch_hours", "approx_unique_ips"],
    )
    data = {
        "title": title,
        "source": "STREAM",
        "generatedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "range": {"start": start, "end": end},
        "defaultRange": {"start": default_start, "end": default_end},
        "summary": summary,
        "rangeHints": best_range_hints(daily_stream),
        "daily": daily_records,
        "statesDaily": records(
            state_daily,
            {
                "log_date": "date",
                "raw_watch_hours": "rawHours",
                "approx_unique_ips": "userDays",
            },
        ),
        "excelFile": "stream_watch_hours_showcase.xlsx",
    }

    excel_frames = {
        # Keep only the formula source sheets the interactive Excel dashboard needs.
        # They are hidden when the workbook is written so stakeholders see one clean tab.
        "Daily Trend": daily_stream[["log_date", "raw_watch_hours", "approx_unique_ips"]].rename(
            columns={
                "log_date": "Date",
                "raw_watch_hours": "Watch Hours",
                "approx_unique_ips": "Approx Users",
            }
        ),
        "State Daily": state_daily.rename(
            columns={
                "log_date": "Date",
                "state": "State",
                "raw_watch_hours": "Watch Hours",
                "approx_unique_ips": "Approx Users",
            }
        ),
    }
    return data, excel_frames


def quote_sheet(sheet_name: str) -> str:
    escaped = sheet_name.replace("'", "''")
    return f"'{escaped}'"


def excel_range(sheet_name: str, col: str, first_row: int, last_row: int) -> str:
    return f"{quote_sheet(sheet_name)}!${col}${first_row}:${col}${last_row}"


def excel_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def prepare_excel_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        if str(col).lower() in {"date", "start", "end"}:
            parsed = pd.to_datetime(out[col], errors="coerce")
            if parsed.notna().any():
                out[col] = parsed.dt.date
    return out


def write_excel_dashboard(
    workbook: Any,
    worksheet: Any,
    frames: dict[str, pd.DataFrame],
    data: dict[str, Any],
) -> None:
    daily_rows = len(frames["Daily Trend"])
    state_daily_rows = len(frames["State Daily"])
    state_names = sorted(str(v) for v in frames["State Daily"]["State"].dropna().unique())
    state_count = len(state_names)

    daily_date = excel_range("Daily Trend", "A", 2, daily_rows + 1)
    daily_hours = excel_range("Daily Trend", "B", 2, daily_rows + 1)
    daily_users = excel_range("Daily Trend", "C", 2, daily_rows + 1)
    state_date = excel_range("State Daily", "A", 2, state_daily_rows + 1)
    state_name = excel_range("State Daily", "B", 2, state_daily_rows + 1)
    state_hours = excel_range("State Daily", "C", 2, state_daily_rows + 1)
    state_users = excel_range("State Daily", "D", 2, state_daily_rows + 1)

    title_fmt = workbook.add_format({"bold": True, "font_size": 22, "font_color": "#172033"})
    label_fmt = workbook.add_format({"bold": True, "font_color": "#5D6678", "font_size": 10})
    date_fmt = workbook.add_format({"num_format": "dd-mm-yyyy", "border": 1, "bg_color": "#FFFFFF"})
    hint_fmt = workbook.add_format({"font_color": "#5D6678", "font_size": 10})
    impact_fmt = workbook.add_format(
        {
            "bold": True,
            "font_size": 40,
            "font_color": "#2563EB",
            "align": "center",
            "valign": "vcenter",
            "bg_color": "#EEF5FF",
            "border": 1,
        }
    )
    impact_label_fmt = workbook.add_format(
        {
            "bold": True,
            "font_size": 18,
            "align": "center",
            "valign": "vcenter",
            "bg_color": "#EEF5FF",
            "border": 1,
        }
    )
    kpi_value_fmt = workbook.add_format(
        {"bold": True, "font_size": 20, "align": "center", "valign": "vcenter", "border": 1, "bg_color": "#FFFFFF"}
    )
    kpi_label_fmt = workbook.add_format(
        {"bold": True, "font_size": 10, "font_color": "#5D6678", "align": "center", "border": 1, "bg_color": "#FFFFFF"}
    )
    header_fmt = workbook.add_format({"bold": True, "font_color": "#5D6678", "bg_color": "#EAF2FF", "border": 1})
    text_fmt = workbook.add_format({"border": 1})
    rank_fmt = workbook.add_format({"border": 1, "align": "right", "num_format": "0"})
    num_fmt = workbook.add_format({"border": 1, "align": "right", "num_format": "#,##0.0"})
    int_fmt = workbook.add_format({"border": 1, "align": "right", "num_format": "#,##0"})
    pct_fmt = workbook.add_format({"border": 1, "align": "right", "num_format": "0.0%"})
    range_fmt = workbook.add_format(
        {
            "bold": True,
            "font_color": "#1D4ED8",
            "bg_color": "#EFF6FF",
            "border": 1,
            "align": "center",
        }
    )

    min_date = excel_date(data["range"]["start"])
    max_date = excel_date(data["range"]["end"])
    min_label = min_date.strftime("%d/%m/%y")
    max_label = max_date.strftime("%d/%m/%y")

    worksheet.activate()
    worksheet.hide_gridlines(2)
    worksheet.set_column("A:A", 6)
    worksheet.set_column("B:B", 28)
    worksheet.set_column("C:C", 13)
    worksheet.set_column("D:D", 16)
    worksheet.set_column("E:E", 17)
    worksheet.set_column("F:F", 2)
    worksheet.set_column("H:K", 16, None, {"hidden": True})

    worksheet.merge_range("A1:E1", data["title"], title_fmt)
    worksheet.write("A3", "Date From", label_fmt)
    worksheet.write("B3", "Date To", label_fmt)
    worksheet.write_datetime("A4", excel_date(data["defaultRange"]["start"]), date_fmt)
    worksheet.write_datetime("B4", excel_date(data["defaultRange"]["end"]), date_fmt)
    worksheet.merge_range("C3:E3", "Available Date Range", label_fmt)
    worksheet.merge_range("C4:E4", f"{min_label} to {max_label}", range_fmt)
    worksheet.merge_range("A5:E5", "Choose any completed date inside this range. The dashboard recalculates after Date From / Date To changes.", hint_fmt)

    date_validation = {
        "validate": "date",
        "criteria": "between",
        "minimum": min_date,
        "maximum": max_date,
        "input_title": "Allowed date range",
        "input_message": f"Choose a completed date from {min_label} to {max_label}.",
        "error_title": "Date outside available data",
        "error_message": f"Use a date between {min_label} and {max_label}.",
        "error_type": "stop",
    }
    for cell in ["A4", "B4"]:
        worksheet.data_validation(cell, date_validation)

    worksheet.write_formula("H4", f'=SUMIFS({daily_hours},{daily_date},">="&$A$4,{daily_date},"<="&$B$4)')
    worksheet.write_formula("H5", "=$H$4*60")
    worksheet.write_formula("H6", f'=SUMIFS({daily_users},{daily_date},">="&$A$4,{daily_date},"<="&$B$4)')
    worksheet.write_formula("H7", f'=COUNTIFS({daily_date},">="&$A$4,{daily_date},"<="&$B$4)')
    worksheet.write_formula("H8", '=IFERROR($H$6/$H$7,0)')
    worksheet.write_formula(
        "H9",
        f'=IFERROR(MAX(INDEX(({daily_date}>=$A$4)*({daily_date}<=$B$4)*{daily_users},0)),0)',
    )
    worksheet.write_formula("H10", '=IFERROR($H$5/$H$6,0)')

    worksheet.merge_range("A7:E9", "", impact_fmt)
    worksheet.write_formula("A7", '=TEXT(FLOOR($H$5/10000000,0.01),"0.00")&" Crore+"', impact_fmt)
    worksheet.merge_range("A10:E11", "minutes of content streamed", impact_label_fmt)

    kpi_cards = [
        ("A13:B13", "A14:B15", "Avg Daily Active Users", '=ROUND($H$8,0)'),
        ("C13:D13", "C14:D15", "Peak Daily Active Users", "=$H$9"),
        ("E13:E13", "E14:E15", "Avg Watch / User-Day", '=ROUND($H$10,1)&" min"'),
    ]
    for label_range, value_range, label, formula in kpi_cards:
        if label_range.split(":")[0] == label_range.split(":")[-1]:
            worksheet.write(label_range, label, kpi_label_fmt)
        else:
            worksheet.merge_range(label_range, label, kpi_label_fmt)
        worksheet.merge_range(value_range, "", kpi_value_fmt)
        worksheet.write_formula(value_range.split(":")[0], formula, kpi_value_fmt)

    calc = workbook.add_worksheet("_State Calc")
    calc.hide()
    calc.write_row(0, 0, ["State", "Watch Hours", "Avg Daily Users", "Share", "Sort Key"], header_fmt)
    for idx, state in enumerate(state_names, start=2):
        calc.write(idx - 1, 0, state)
        watch_formula = (
            f'=SUMIFS({state_hours},{state_date},">="&Dashboard!$A$4,'
            f'{state_date},"<="&Dashboard!$B$4,{state_name},$A{idx})'
        )
        user_formula = (
            f'=IFERROR(SUMIFS({state_users},{state_date},">="&Dashboard!$A$4,'
            f'{state_date},"<="&Dashboard!$B$4,{state_name},$A{idx})/Dashboard!$H$7,0)'
        )
        calc.write_formula(idx - 1, 1, watch_formula)
        calc.write_formula(idx - 1, 2, user_formula)
        calc.write_formula(idx - 1, 3, f'=IFERROR(B{idx}/SUM($B$2:$B${state_count + 1}),0)')
        calc.write_formula(idx - 1, 4, f'=B{idx}+ROW()/1000000000000')

    worksheet.write("A17", "India State Ranking", title_fmt)
    worksheet.write_row("A19", ["#", "State", "Share", "Watch Hours", "Avg Daily Users"], header_fmt)

    sort_range = f"{quote_sheet('_State Calc')}!$E$2:$E${state_count + 1}"

    def index_formula(rank: int, col: str) -> str:
        idx = f"MATCH(LARGE({sort_range},{rank}),{sort_range},0)"
        return f"=INDEX({quote_sheet('_State Calc')}!${col}$2:${col}${state_count + 1},{idx})"

    first_rank_row = 20
    visible_count = min(10, state_count)

    def write_state_rank(excel_row: int, rank: int, hidden: bool = False) -> None:
        row_zero = excel_row - 1
        row_options = {"level": 1, "hidden": True} if hidden else {}
        worksheet.set_row(row_zero, None, None, row_options or {})
        worksheet.write_number(row_zero, 0, rank, rank_fmt)
        worksheet.write_formula(row_zero, 1, index_formula(rank, "A"), text_fmt)
        worksheet.write_formula(row_zero, 2, index_formula(rank, "D"), pct_fmt)
        worksheet.write_formula(row_zero, 3, index_formula(rank, "B"), num_fmt)
        worksheet.write_formula(row_zero, 4, index_formula(rank, "C"), int_fmt)

    for rank in range(1, visible_count + 1):
        write_state_rank(first_rank_row + rank - 1, rank)

    if state_count > 10:
        label_row = first_rank_row + visible_count
        worksheet.merge_range(label_row - 1, 0, label_row - 1, 4, "Expand grouped rows to view remaining states", hint_fmt)
        worksheet.set_row(label_row - 1, None, None, {"collapsed": True})
        for rank in range(11, state_count + 1):
            write_state_rank(label_row + rank - 10, rank, hidden=True)


def write_excel(path: Path, frames: dict[str, pd.DataFrame], data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
        workbook = writer.book
        workbook.set_calc_mode("auto")
        dashboard = workbook.add_worksheet("Dashboard")
        writer.sheets["Dashboard"] = dashboard
        header_fmt = workbook.add_format({"bold": True, "bg_color": "#EAF2FF", "border": 1})
        number_fmt = workbook.add_format({"num_format": "#,##0.0"})
        int_fmt = workbook.add_format({"num_format": "#,##0"})
        date_fmt = workbook.add_format({"num_format": "dd-mm-yyyy"})
        for sheet, df in frames.items():
            safe_sheet = sheet[:31]
            output_df = prepare_excel_frame(df)
            output_df.to_excel(writer, sheet_name=safe_sheet, index=False)
            worksheet = writer.sheets[safe_sheet]
            if safe_sheet in {"Daily Trend", "State Daily"}:
                worksheet.hide()
            for idx, col in enumerate(output_df.columns):
                worksheet.write(0, idx, col, header_fmt)
                sample = output_df[col].astype(str).head(50).map(len).max() if not output_df.empty else 10
                width = min(max(len(str(col)) + 2, int(sample) + 2, 12), 38)
                fmt = None
                if str(col).lower() in {"date", "start", "end"}:
                    fmt = date_fmt
                elif pd.api.types.is_integer_dtype(output_df[col]):
                    fmt = int_fmt
                elif pd.api.types.is_float_dtype(output_df[col]):
                    fmt = number_fmt
                worksheet.set_column(idx, idx, width, fmt)
            worksheet.freeze_panes(1, 0)
        write_excel_dashboard(workbook, dashboard, frames, data)


def render_html(template_path: Path, data: dict[str, Any], chartjs: str) -> str:
    text = template_path.read_text(encoding="utf-8")
    return text.replace("__DATA_BLOB__", json_blob(data)).replace("__CHARTJS__", chartjs)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate STREAM watch-hours PR showcase HTML and Excel.")
    parser.add_argument("--watch-root", default=str(DEFAULT_WATCH_ROOT), help="ETL/output/watch_hours folder")
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR), help="Output folder for HTML and Excel")
    parser.add_argument("--title", default="STREAM Watch Hours Showcase", help="Dashboard title")
    parser.add_argument("--dry-run", action="store_true", help="Validate inputs without writing files")
    args = parser.parse_args()

    watch_root = Path(args.watch_root).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    html_out = out_dir / "stream_watch_hours_showcase.html"
    excel_out = out_dir / "stream_watch_hours_showcase.xlsx"

    print(f"Watch root: {watch_root}")
    print(f"Output dir: {out_dir}")
    data, excel_frames = build_data(watch_root, args.title)

    if args.dry_run:
        print(f"Dry run OK. Range: {data['range']['start']} to {data['range']['end']}")
        print(f"Daily rows: {len(data['daily']):,}")
        return

    write_excel(excel_out, excel_frames, data)
    chartjs = load_chartjs(DEFAULT_CHARTJS_CACHE)
    html = render_html(HERE / "template.html", data, chartjs)
    atomic_write_text(html_out, html)

    print(f"HTML written : {html_out}")
    print(f"Excel written: {excel_out}")
    print(f"Range        : {data['range']['start']} to {data['range']['end']}")


if __name__ == "__main__":
    main()
