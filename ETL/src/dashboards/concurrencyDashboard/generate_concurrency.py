#!/usr/bin/env python3
"""Generate a standalone dark-mode FAST concurrency dashboard."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd


HERE = Path(__file__).resolve().parent


def resolve_src_root() -> Path:
    env_root = os.getenv("VG_ETL_SRC_ROOT")
    candidates = [Path(env_root).expanduser().resolve()] if env_root else []
    candidates.extend(list(HERE.parents)[:6])
    for candidate in candidates:
        if (candidate / "common" / "chartjs.py").exists() and (candidate / "common" / "render.py").exists():
            return candidate
    raise FileNotFoundError(
        f"Could not locate ETL src/common helpers from {HERE}. "
        "Set VG_ETL_SRC_ROOT to the ETL/src directory."
    )


SRC_ROOT = resolve_src_root()
ETL_ROOT = SRC_ROOT.parent


def _load_common_module(module_name: str, file_name: str) -> Any:
    path = SRC_ROOT / "common" / file_name
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load {module_name} from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_chartjs_module = _load_common_module("veto_common_chartjs", "chartjs.py")
_render_module = _load_common_module("veto_common_render", "render.py")

load_chartjs = _chartjs_module.load_chartjs
chartjs_script = _render_module.chartjs_script
json_blob = _render_module.json_blob
render_template = _render_module.render_template

DEFAULT_CHARTJS_CACHE = ETL_ROOT / "output" / "cache" / "chartjs" / "chart.umd.min.js"


DEFAULT_DATA_DIR = Path(
    os.getenv(
        "VG_CONCURRENCY_DIR",
        str(ETL_ROOT / "output" / "watch_hours" / "concurrency"),
    )
)
DEFAULT_OUT = Path(
    os.getenv(
        "VG_CONCURRENCY_HTML",
        str(DEFAULT_DATA_DIR / "veto_concurrency.html"),
    )
)
IST = ZoneInfo("Asia/Kolkata")
STATUS_CODE_MEANINGS = {
    "000": "Non-standard: no HTTP response code was logged; commonly indicates the request closed before a normal response was recorded.",
    "200": "OK: request succeeded.",
    "206": "Partial Content: byte-range response, commonly used for media segment delivery.",
    "301": "Moved Permanently: client should use the redirected URL.",
    "302": "Found: temporary redirect.",
    "304": "Not Modified: cache validation response; payload not sent again.",
    "400": "Bad Request: malformed or invalid request.",
    "401": "Unauthorized: authentication is required or failed.",
    "403": "Forbidden: server understood the request but refused access.",
    "404": "Not Found: requested object was not available.",
    "408": "Request Timeout: client did not complete the request in time.",
    "429": "Too Many Requests: rate limit or throttling response.",
    "500": "Internal Server Error: origin/server-side failure.",
    "501": "Not Implemented: method or feature not supported by server.",
    "502": "Bad Gateway: upstream server returned an invalid response.",
    "503": "Service Unavailable: server or upstream temporarily unavailable.",
    "504": "Gateway Timeout: upstream did not respond in time.",
}


class ParquetReadError(RuntimeError):
    """Raised when an existing parquet file cannot be read."""


def warn(message: str) -> None:
    print(f"[warn] {message}")


def temp_path_for(target: Path) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    handle, temp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
    os.close(handle)
    return Path(temp_name)


def atomic_write_text(path: Path, text: str, encoding: str = "utf-8") -> None:
    temp_path = None
    try:
        temp_path = temp_path_for(path)
        temp_path.write_text(text, encoding=encoding)
        os.replace(temp_path, path)
    finally:
        if temp_path is not None and temp_path.exists():
            temp_path.unlink()


def atomic_write_exports(export: pd.DataFrame, csv_path: Path, parquet_path: Path) -> None:
    csv_tmp = None
    parquet_tmp = None
    try:
        csv_tmp = temp_path_for(csv_path)
        parquet_tmp = temp_path_for(parquet_path)
        export.to_csv(csv_tmp, index=False, encoding="utf-8")
        export.to_parquet(parquet_tmp, index=False, compression="zstd")
        # Each output file is replaced atomically, but the CSV/parquet pair is
        # not a single atomic transaction. Parquet is the machine-readable
        # primary output, and CSV is a convenience export, so publish parquet
        # last to make the primary file the final visible update.
        os.replace(csv_tmp, csv_path)
        os.replace(parquet_tmp, parquet_path)
    finally:
        for temp_path in (csv_tmp, parquet_tmp):
            if temp_path is not None and temp_path.exists():
                temp_path.unlink()


def read_json(path: Path) -> dict:
    data, _ = read_json_with_status(path)
    return data


def read_json_with_status(path: Path) -> tuple[dict, str]:
    if not path.exists():
        return {}, ""
    try:
        return json.loads(path.read_text(encoding="utf-8")), ""
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        message = f"Could not read manifest JSON {path}: {exc}"
        warn(message)
        return {}, message


def read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Required parquet not found: {path}")
    try:
        return pd.read_parquet(path)
    except Exception as exc:
        raise ParquetReadError(f"Could not read parquet file {path}: {exc}") from exc


def read_optional_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception as exc:
        raise ParquetReadError(f"Could not read optional parquet file {path}: {exc}") from exc


def latest_completed_ist_date() -> str:
    """Static concurrency reports should only expose fully completed IST dates."""
    return (datetime.now(IST).date() - timedelta(days=1)).isoformat()


def filter_completed_frame(df: pd.DataFrame, date_col: str = "log_date") -> pd.DataFrame:
    if df.empty or date_col not in df.columns:
        return df
    dates = pd.to_datetime(df[date_col], errors="coerce").dt.strftime("%Y-%m-%d")
    keep = dates.le(latest_completed_ist_date()).fillna(False)
    return df.loc[keep].copy()


def clean_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    text_cols = [
        "log_date",
        "source",
        "minute_utc",
        "minute_ist",
        "reqHost",
        "platform_key",
        "platform_name",
        "candidate_id",
        "channel_name",
        "status_code",
        "peak_unique_viewers_minute_ist",
        "peak_unique_ua_minute_ist",
        "peak_segment_minute_ist",
    ]
    for col in text_cols:
        if col in out.columns:
            out[col] = out[col].fillna("").astype(str)
    if "pair_key" not in out.columns and not out.empty:
        out["pair_key"] = build_pair_key(out)
    return out


def build_pair_key(df: pd.DataFrame) -> pd.Series:
    parts = []
    for name in ["source", "platform_key", "candidate_id", "channel_name"]:
        if name in df.columns:
            parts.append(df[name].fillna("").astype(str))
        else:
            parts.append(pd.Series([""] * len(df), index=df.index, dtype=str))

    raw = pd.Series("", index=df.index, dtype=str)
    for part in parts:
        raw = raw + part.str.len().astype(str) + ":" + part
    return raw


def records(df: pd.DataFrame, columns: list[str]) -> list[dict]:
    if df.empty:
        return []
    out = df[[col for col in columns if col in df.columns]].copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            if getattr(out[col].dt, "tz", None) is not None:
                out[col] = out[col].dt.tz_convert(IST).dt.strftime("%Y-%m-%d %H:%M:%S")
            else:
                out[col] = out[col].dt.strftime("%Y-%m-%d %H:%M:%S")
    out = out.astype(object).where(pd.notna(out), None)
    return out.to_dict(orient="records")


def array_records(df: pd.DataFrame, columns: list[str]) -> list[list]:
    """Return row arrays plus a separate schema to avoid repeating JSON keys millions of times."""
    if df.empty:
        return []
    out = df[[col for col in columns if col in df.columns]].copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            if getattr(out[col].dt, "tz", None) is not None:
                out[col] = out[col].dt.tz_convert(IST).dt.strftime("%Y-%m-%d %H:%M:%S")
            else:
                out[col] = out[col].dt.strftime("%Y-%m-%d %H:%M:%S")
    out = out.astype(object).where(pd.notna(out), None)
    return out.values.tolist()


def dictionary_array_records(
    df: pd.DataFrame,
    columns: list[str],
    dictionary_columns: list[str],
) -> tuple[dict[str, list[str]], list[list]]:
    """Encode repeated text columns as integer dictionary ids before embedding in HTML."""
    if df.empty:
        return {}, []
    out = df[[col for col in columns if col in df.columns]].copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            if getattr(out[col].dt, "tz", None) is not None:
                out[col] = out[col].dt.tz_convert(IST).dt.strftime("%Y-%m-%d %H:%M:%S")
            else:
                out[col] = out[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    dictionaries: dict[str, list[str]] = {}
    for col in dictionary_columns:
        if col not in out.columns:
            continue
        values = out[col].where(pd.notna(out[col]), None)
        labels = pd.Series(values.dropna().astype(str).unique()).tolist()
        mapping = {label: idx for idx, label in enumerate(labels)}
        out[col] = values.map(lambda value: mapping.get(str(value)) if value is not None else None)
        dictionaries[col] = labels

    out = out.astype(object).where(pd.notna(out), None)
    return dictionaries, out.values.tolist()


def build_status_payload(status_minute: pd.DataFrame) -> tuple[list[dict], dict[str, list[str]], list[list]]:
    """Return status-code filter metadata plus sparse non-200 minute values."""
    if status_minute.empty or "status_code" not in status_minute.columns:
        return [], {}, []

    required = {
        "pair_key",
        "minute_ist",
        "status_code",
        "status_ts_rows",
        "status_segment_viewers_estimate",
    }
    if not required.issubset(status_minute.columns):
        return [], {}, []

    status = status_minute.copy()
    status["status_code"] = status["status_code"].fillna("Unknown").astype(str)
    status_codes = sorted(
        status["status_code"].dropna().astype(str).unique(),
        key=lambda value: (not value.isdigit(), value),
    )
    options = [
        {
            "code": code,
            "meaning": STATUS_CODE_MEANINGS.get(
                code,
                "Observed in logs; no built-in reference meaning is configured yet.",
            ),
        }
        for code in status_codes
    ]

    extra = status[status["status_code"] != "200"].copy()
    if extra.empty:
        return options, {}, []

    extra["status_ts_rows"] = (
        pd.to_numeric(extra["status_ts_rows"], errors="coerce")
        .fillna(0)
        .round()
        .astype("int64")
    )
    extra["status_segment_viewers_estimate"] = pd.to_numeric(
        extra["status_segment_viewers_estimate"],
        errors="coerce",
    ).fillna(0.0)
    extra = (
        extra.groupby(["pair_key", "minute_ist", "status_code"], as_index=False, sort=False)
        .agg(
            status_ts_rows=("status_ts_rows", "sum"),
            status_segment_viewers_estimate=("status_segment_viewers_estimate", "sum"),
        )
        .sort_values(["minute_ist", "pair_key", "status_code"])
    )
    extra = extra.rename(
        columns={
            "pair_key": "k",
            "minute_ist": "m",
            "status_code": "c",
            "status_ts_rows": "r",
            "status_segment_viewers_estimate": "v",
        }
    )
    dictionaries, rows = dictionary_array_records(extra, ["k", "m", "c", "r", "v"], ["k", "m", "c"])
    return options, dictionaries, rows


def write_viewer_exports(
    data_dir: Path,
    minute: pd.DataFrame,
    *,
    source_column: str,
    output_stem: str,
    viewer_label: str,
    dry_run: bool = False,
) -> dict:
    csv_path = data_dir / f"{output_stem}.csv"
    parquet_path = data_dir / f"{output_stem}.parquet"
    viewer_column = f"Number of viewers({viewer_label})"
    required = {"minute_ist", "channel_name", "platform_name", source_column}
    if minute.empty or not required.issubset(minute.columns):
        empty = pd.DataFrame(
            columns=[
                "Timestamp (IST)",
                "Channel name",
                "Platform name",
                viewer_column,
            ]
        )
        if not dry_run:
            atomic_write_exports(empty, csv_path, parquet_path)
        return {"csv": str(csv_path), "parquet": str(parquet_path), "rows": 0}

    export = minute[
        ["minute_ist", "channel_name", "platform_name", source_column]
    ].rename(
        columns={
            "minute_ist": "Timestamp (IST)",
            "channel_name": "Channel name",
            "platform_name": "Platform name",
            source_column: viewer_column,
        }
    )
    export[viewer_column] = (
        pd.to_numeric(export[viewer_column], errors="coerce")
        .fillna(0)
        .round()
        .astype("int64")
    )
    export = (
        export.groupby(
            ["Timestamp (IST)", "Channel name", "Platform name"],
            as_index=False,
            sort=False,
        )[viewer_column]
        .sum()
        .sort_values(["Timestamp (IST)", "Platform name", "Channel name"])
    )
    if not dry_run:
        atomic_write_exports(export, csv_path, parquet_path)
    return {"csv": str(csv_path), "parquet": str(parquet_path), "rows": int(len(export))}


def write_ua_viewer_exports(data_dir: Path, minute: pd.DataFrame, dry_run: bool = False) -> dict:
    return write_viewer_exports(
        data_dir,
        minute,
        source_column="unique_ua_viewers",
        output_stem="concurrency_ua_viewers",
        viewer_label="UA",
        dry_run=dry_run,
    )


def write_cliip_viewer_exports(data_dir: Path, minute: pd.DataFrame, dry_run: bool = False) -> dict:
    return write_viewer_exports(
        data_dir,
        minute,
        source_column="unique_viewers",
        output_stem="concurrency_cliip_viewers",
        viewer_label="cliIP",
        dry_run=dry_run,
    )


def _numeric_column(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series([0] * len(df), index=df.index, dtype="float64")
    return pd.to_numeric(df[column], errors="coerce").fillna(0)


def build_data(data_dir: Path, title: str, embed_window_days: int = 30) -> tuple[dict, pd.DataFrame]:
    minute_path = data_dir / "concurrency_minute.parquet"
    status_minute_path = data_dir / "concurrency_status_minute.parquet"
    summary_path = data_dir / "concurrency_summary.parquet"
    manifest_path = data_dir / "concurrency_manifest.json"

    manifest, manifest_error = read_json_with_status(manifest_path)
    minute = clean_frame(read_parquet(minute_path))
    status_minute = clean_frame(read_optional_parquet(status_minute_path))
    summary = clean_frame(read_parquet(summary_path))
    minute = filter_completed_frame(minute)
    status_minute = filter_completed_frame(status_minute)
    summary = filter_completed_frame(summary)
    if not status_minute.empty:
        status_minute = status_minute.sort_values(
            ["log_date", "platform_name", "channel_name", "candidate_id", "status_code", "minute_ist"]
        )
    if not minute.empty:
        minute = minute.sort_values(["log_date", "platform_name", "channel_name", "candidate_id", "minute_ist"])
    if not summary.empty:
        summary = summary.sort_values(["log_date", "platform_name", "channel_name", "candidate_id"])
    full_source_dates = (
        sorted(str(day) for day in minute["log_date"].dropna().unique())
        if not minute.empty and "log_date" in minute.columns
        else []
    )
    if embed_window_days and embed_window_days > 0 and len(full_source_dates) > embed_window_days:
        embedded_dates = set(full_source_dates[-embed_window_days:])
        minute = minute[minute["log_date"].astype(str).isin(embedded_dates)]
        if not status_minute.empty and "log_date" in status_minute.columns:
            status_minute = status_minute[status_minute["log_date"].astype(str).isin(embedded_dates)]
        if not summary.empty and "log_date" in summary.columns:
            summary = summary[summary["log_date"].astype(str).isin(embedded_dates)]
    status_code_options, status_extra_dictionaries, status_extra = build_status_payload(status_minute)

    integrity_checks = []
    if not minute.empty and not status_minute.empty:
        minute_total = int(_numeric_column(minute, "raw_ts_rows").sum())
        status_total = int(_numeric_column(status_minute, "status_ts_rows").sum())
        integrity_checks.append(
            {
                "name": "minute_vs_status_ts_rows",
                "status": "ok" if minute_total == status_total else "mismatch",
                "minute_total": minute_total,
                "status_total": status_total,
                "diff": minute_total - status_total,
            }
        )
    if not minute.empty and not summary.empty:
        minute_total = int(_numeric_column(minute, "raw_ts_rows").sum())
        summary_total = int(_numeric_column(summary, "raw_ts_rows").sum())
        integrity_checks.append(
            {
                "name": "minute_vs_summary_ts_rows",
                "status": "ok" if minute_total == summary_total else "mismatch",
                "minute_total": minute_total,
                "summary_total": summary_total,
                "diff": minute_total - summary_total,
            }
        )
    if not integrity_checks:
        integrity_status = "Unknown"
    else:
        integrity_status = "OK" if all(item["status"] == "ok" for item in integrity_checks) else "Check"

    dates = (
        sorted(str(day) for day in minute["log_date"].dropna().unique())
        if not minute.empty and "log_date" in minute.columns
        else []
    )
    minute_times = (
        sorted(str(value) for value in minute["minute_ist"].dropna().unique())
        if not minute.empty and "minute_ist" in minute.columns
        else []
    )
    current_ist_date = datetime.now(IST).date().isoformat()
    full_dates = [day for day in dates if day < current_ist_date]
    latest_full_date = full_dates[-1] if full_dates else ""

    stats = {
        "minute_rows": int(len(minute)),
        "status_extra_rows": int(len(status_extra)),
        "summary_rows": int(len(summary)),
        "first_date": dates[0] if dates else "",
        "last_date": dates[-1] if dates else "",
        "first_ist": minute_times[0] if minute_times else "",
        "last_ist": minute_times[-1] if minute_times else "",
        "latest_full_date": latest_full_date,
        "current_ist_date": current_ist_date,
        "current_date_hidden": bool(latest_full_date and current_ist_date in dates),
        "dates": len(dates),
        "platforms": int(minute["platform_name"].nunique()) if not minute.empty and "platform_name" in minute.columns else 0,
        "channels": int(minute["channel_name"].nunique()) if not minute.empty and "channel_name" in minute.columns else 0,
        "pairs": int(minute[["platform_key", "candidate_id", "channel_name"]].drop_duplicates().shape[0])
        if not minute.empty and {"platform_key", "candidate_id", "channel_name"}.issubset(minute.columns)
        else 0,
        "integrity_status": integrity_status,
        "integrity_checks": integrity_checks,
        "manifest_read_error": manifest_error,
        "full_first_date": full_source_dates[0] if full_source_dates else "",
        "full_last_date": full_source_dates[-1] if full_source_dates else "",
        "embed_window_days": int(embed_window_days or 0),
        "peak_unique_viewers": int(_numeric_column(minute, "unique_viewers").max() or 0)
        if not minute.empty
        else 0,
        "peak_unique_ua_viewers": int(_numeric_column(minute, "unique_ua_viewers").max() or 0)
        if not minute.empty
        else 0,
    }
    minute_columns = [
        "log_date",
        "source",
        "minute_ist",
        "platform_key",
        "platform_name",
        "candidate_id",
        "channel_name",
        "raw_ts_rows",
        "status_200_ts_rows",
        "unique_viewers",
        "unique_ua_viewers",
        "segment_viewers_estimate",
        "status_200_segment_viewers_estimate",
        "pair_key",
    ]

    minute_dictionaries, minute_rows = dictionary_array_records(
        minute,
        minute_columns,
        ["source", "minute_ist", "platform_key", "platform_name", "candidate_id", "channel_name", "pair_key"],
    )

    data = {
        "title": title,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data_dir": str(data_dir),
        "files": {
            "minute": str(minute_path),
            "status_minute": str(status_minute_path) if status_minute_path.exists() else "",
            "summary": str(summary_path),
            "manifest": str(manifest_path),
        },
        "stats": stats,
        "manifest": manifest,
        "status_meanings": STATUS_CODE_MEANINGS,
        "status_code_options": status_code_options,
        "status_extra_schema": ["k", "m", "c", "r", "v"],
        "status_extra_dictionaries": status_extra_dictionaries,
        "status_extra": status_extra,
        "minute_schema": minute_columns,
        "minute_dictionaries": minute_dictionaries,
        "minute": minute_rows,
        "summary": records(
            summary,
            [
                "log_date",
                "source",
                "reqHost",
                "platform_key",
                "platform_name",
                "candidate_id",
                "channel_name",
                "distinct_hosts",
                "minute_count",
                "raw_ts_rows",
                "status_200_ts_rows",
                "avg_unique_viewers",
                "peak_unique_viewers",
                "peak_unique_viewers_minute_ist",
                "p95_unique_viewers",
                "avg_unique_ua_viewers",
                "peak_unique_ua_viewers",
                "peak_unique_ua_minute_ist",
                "p95_unique_ua_viewers",
                "avg_segment_viewers_estimate",
                "peak_segment_viewers_estimate",
                "peak_segment_minute_ist",
                "avg_status_200_segment_viewers_estimate",
                "peak_status_200_segment_viewers_estimate",
                "pair_key",
            ],
        ),
    }
    return data, minute


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate standalone FAST concurrency HTML dashboard.")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--title", default="Veto Concurrency")
    parser.add_argument("--embed-window-days", type=int, default=30, help="Embed only the latest N days in HTML; use 0 for full history.")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    data_dir = args.data_dir.expanduser().resolve()
    out_path = args.out.expanduser().resolve()
    data, minute_for_export = build_data(data_dir, args.title, args.embed_window_days)
    data["files"]["ua_viewers_export"] = write_ua_viewer_exports(
        data_dir,
        minute_for_export,
        dry_run=args.dry_run,
    )
    data["files"]["cliip_viewers_export"] = write_cliip_viewer_exports(
        data_dir,
        minute_for_export,
        dry_run=args.dry_run,
    )
    if args.dry_run:
        stats = data.get("stats", {})
        print(f"[Dry run] Concurrency data OK - {stats.get('minute_rows', 0):,} minute rows")
        print(f"  Would write: {out_path}")
        return

    chartjs = load_chartjs(DEFAULT_CHARTJS_CACHE, fallback="window.Chart=null;")
    html = render_template(
        HERE / "template.html",
        DATA_BLOB=json_blob(data),
        CHARTJS_TAG=chartjs_script(chartjs),
    )

    atomic_write_text(out_path, html, encoding="utf-8")
    print(f"Concurrency dashboard written: {out_path}")
    print(f"Size: {out_path.stat().st_size / 1024:.1f} KB")


if __name__ == "__main__":
    main()
