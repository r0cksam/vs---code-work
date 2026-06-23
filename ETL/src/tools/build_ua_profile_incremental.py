#!/usr/bin/env python3
"""Build distinct User-Agent profile parquet incrementally from lake partitions."""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime
from pathlib import Path

import duckdb
import pandas as pd


ETL_ROOT = Path(__file__).resolve().parents[2]
TOOLS_ROOT = ETL_ROOT / "src" / "tools"
if str(TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(TOOLS_ROOT))

import profile_user_agents as ua_profile  # noqa: E402


DEFAULT_LAKE = ETL_ROOT / "data" / "lake"
DEFAULT_OUT = ETL_ROOT / "output" / "device_decode"
DEFAULT_PARTS = DEFAULT_OUT / "ua_profile_parts"


def log(message: str) -> None:
    try:
        print(f"[{datetime.now().isoformat(timespec='seconds')}] {message}", flush=True)
    except OSError:
        return


def parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def q(path: Path | str) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def sql_text(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def day_path(lake: Path, source: str, day_value: date) -> Path:
    return (
        lake
        / f"source={source}"
        / f"year={day_value:%Y}"
        / f"month={day_value:%m}"
        / f"day={day_value:%d}"
    )


def part_dir(parts_root: Path, source: str, day_value: date) -> Path:
    return (
        parts_root
        / f"source={source}"
        / f"year={day_value:%Y}"
        / f"month={day_value:%m}"
        / f"day={day_value:%d}"
    )


def discover_days(lake: Path, sources: list[str], start: date | None, end: date | None) -> list[tuple[str, date]]:
    jobs: list[tuple[str, date]] = []
    for source in sources:
        root = lake / f"source={source}"
        for folder in root.glob("year=*/month=*/day=*"):
            if not folder.is_dir() or not any(folder.glob("*.parquet")):
                continue
            try:
                values = {
                    item.split("=", 1)[0]: item.split("=", 1)[1]
                    for item in folder.parts
                    if "=" in item
                }
                day_value = date(int(values["year"]), int(values["month"]), int(values["day"]))
            except (KeyError, ValueError, IndexError):
                continue
            if start and day_value < start:
                continue
            if end and day_value > end:
                continue
            jobs.append((source, day_value))
    return sorted(set(jobs), key=lambda item: (item[1], item[0]))


def source_list(value: str) -> list[str]:
    if value == "both":
        return ["fast", "stream"]
    return [value]


def signature_for_day(lake: Path, source: str, day_value: date) -> dict:
    files = sorted(day_path(lake, source, day_value).glob("*.parquet"))
    total_bytes = 0
    max_mtime_ns = 0
    for file in files:
        stat = file.stat()
        total_bytes += int(stat.st_size)
        max_mtime_ns = max(max_mtime_ns, int(stat.st_mtime_ns))
    return {
        "file_count": len(files),
        "total_bytes": total_bytes,
        "max_mtime_ns": max_mtime_ns,
    }


def read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.stem}.tmp{path.suffix}")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def atomic_write_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.stem}.tmp{path.suffix}")
    tmp.unlink(missing_ok=True)
    frame.to_parquet(tmp, index=False, compression="zstd")
    tmp.replace(path)


def day_sql(args: argparse.Namespace, source: str, day_value: date) -> str:
    folder = day_path(args.lake, source, day_value)
    parquet_glob = q(folder / "*.parquet")
    day_expr = ua_profile.ist_date_expr("reqTimeSec")
    day_sql_literal = sql_text(day_value.isoformat())
    source_literal = sql_text(source)
    return f"""
    WITH base AS (
        SELECT
            CAST({day_expr} AS VARCHAR) AS log_date,
            lower(COALESCE(CAST(source AS VARCHAR), {source_literal})) AS source,
            UA AS ua_raw,
            cliIP,
            lower(reqHost) AS reqHost,
            reqPath,
            regexp_replace(CAST(statusCode AS VARCHAR), '\\.0$', '') AS statusCode,
            lower(reqPath) LIKE '%.ts' AS is_ts
        FROM read_parquet('{parquet_glob}', hive_partitioning=1, union_by_name=1)
        WHERE {day_expr} = DATE {day_sql_literal}
          AND NULLIF(UA, '') IS NOT NULL
    )
    SELECT
        source,
        ua_raw,
        COUNT(*)::BIGINT AS rows,
        SUM(CASE WHEN is_ts THEN 1 ELSE 0 END)::BIGINT AS ts_rows,
        SUM(CASE WHEN is_ts AND statusCode = '200' THEN 1 ELSE 0 END)::BIGINT AS status_200_ts_rows,
        COUNT(DISTINCT NULLIF(cliIP, ''))::BIGINT AS approx_ips,
        MIN(log_date) AS first_date,
        MAX(log_date) AS last_date,
        ANY_VALUE(reqHost) AS sample_reqHost,
        ANY_VALUE(reqPath) AS sample_reqPath
    FROM base
    GROUP BY source, ua_raw
    ORDER BY rows DESC
    """


def connect(args: argparse.Namespace) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET threads={max(1, int(args.threads))}")
    con.execute(f"SET memory_limit={sql_text(args.memory_limit)}")
    con.execute("SET preserve_insertion_order=false")
    if args.temp_dir:
        args.temp_dir.mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory={sql_text(q(args.temp_dir))}")
    return con


def process_day(args: argparse.Namespace, source: str, day_value: date) -> dict:
    folder = part_dir(args.parts_dir, source, day_value)
    out_path = folder / "ua_distinct.parquet"
    manifest_path = folder / "manifest.json"
    signature = signature_for_day(args.lake, source, day_value)
    old_manifest = read_json(manifest_path)
    if (
        not args.force
        and out_path.exists()
        and old_manifest.get("signature") == signature
        and out_path.stat().st_size > 0
    ):
        return {
            "status": "skipped",
            "rows": int(old_manifest.get("distinct_ua", 0) or 0),
            "raw_rows": int(old_manifest.get("raw_rows", 0) or 0),
        }

    started = time.time()
    con = connect(args)
    raw = con.execute(day_sql(args, source, day_value)).fetchdf()
    con.close()
    profile = ua_profile.collapse_profile(raw)
    if not args.dry_run:
        atomic_write_parquet(profile, out_path)
        write_json(
            manifest_path,
            {
                "source": source,
                "log_date": day_value.isoformat(),
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "signature": signature,
                "distinct_ua": int(len(profile)),
                "raw_rows": int(profile["rows"].sum()) if "rows" in profile else 0,
                "elapsed_seconds": round(time.time() - started, 2),
            },
        )
    return {
        "status": "processed",
        "rows": int(len(profile)),
        "raw_rows": int(profile["rows"].sum()) if "rows" in profile else 0,
    }


def merge_parts(args: argparse.Namespace, jobs: list[tuple[str, date]]) -> tuple[pd.DataFrame, Path]:
    part_files = []
    job_set = set(jobs)
    for source, day_value in job_set:
        file = part_dir(args.parts_dir, source, day_value) / "ua_distinct.parquet"
        if file.exists() and file.stat().st_size > 0:
            part_files.append(file)
    if not part_files:
        raise SystemExit("No UA profile parts were available to merge.")

    frames = [pd.read_parquet(file) for file in sorted(part_files)]
    all_parts = pd.concat(frames, ignore_index=True)
    for col in ["rows", "ts_rows", "status_200_ts_rows", "approx_ips"]:
        all_parts[col] = pd.to_numeric(all_parts[col], errors="coerce").fillna(0)

    merged = (
        all_parts.groupby(["source", "ua_hash", "ua_norm_key"], as_index=False)
        .agg(
            ua_sample=("ua_sample", "first"),
            rows=("rows", "sum"),
            ts_rows=("ts_rows", "sum"),
            status_200_ts_rows=("status_200_ts_rows", "sum"),
            approx_ips=("approx_ips", "sum"),
            first_date=("first_date", "min"),
            last_date=("last_date", "max"),
            sample_reqHost=("sample_reqHost", "first"),
            sample_reqPath=("sample_reqPath", "first"),
        )
        .sort_values("rows", ascending=False)
    )
    merged["watch_hours"] = merged["ts_rows"] * ua_profile.CHUNK_DURATION_HOURS
    merged["status_200_watch_hours"] = (
        merged["status_200_ts_rows"] * ua_profile.CHUNK_DURATION_HOURS
    )
    merged = merged[
        [
            "source",
            "ua_hash",
            "ua_norm_key",
            "ua_sample",
            "rows",
            "ts_rows",
            "status_200_ts_rows",
            "watch_hours",
            "status_200_watch_hours",
            "approx_ips",
            "first_date",
            "last_date",
            "sample_reqHost",
            "sample_reqPath",
        ]
    ]

    first_day = min(day for _, day in jobs).isoformat()
    last_day = max(day for _, day in jobs).isoformat()
    suffix = f"{args.source}_sources" if args.source != "both" else "all_sources"
    out_path = args.out_dir / f"ua_distinct_profile_{suffix}_{first_day}_to_{last_day}.parquet"
    if not args.dry_run:
        atomic_write_parquet(merged, out_path)
        write_json(
            args.out_dir / f"ua_distinct_profile_incremental_manifest_{suffix}_{first_day}_to_{last_day}.json",
            {
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "lake": str(args.lake),
                "parts_dir": str(args.parts_dir),
                "output": str(out_path),
                "source": args.source,
                "first_date": first_day,
                "last_date": last_day,
                "source_days": len(jobs),
                "part_files": len(part_files),
                "distinct_ua": int(len(merged)),
                "raw_rows": int(merged["rows"].sum()),
                "approx_ips_note": "sum of per-day distinct IP counts; not true cross-day distinct IP",
            },
        )
    return merged, out_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Incrementally build distinct-UA profile from lake day partitions.")
    parser.add_argument("--lake", type=Path, default=DEFAULT_LAKE)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--parts-dir", type=Path, default=DEFAULT_PARTS)
    parser.add_argument("--source", choices=["both", "stream", "fast"], default="both")
    parser.add_argument("--start", default=None, help="IST start date YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="IST end date YYYY-MM-DD")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory-limit", default="12GB")
    parser.add_argument("--temp-dir", type=Path, default=ETL_ROOT / "output" / "cache" / "duckdb_temp")
    parser.add_argument("--force", action="store_true", help="Rebuild existing daily parts even if signatures match.")
    parser.add_argument("--skip-merge", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    args.lake = args.lake.expanduser().resolve()
    args.out_dir = args.out_dir.expanduser().resolve()
    args.parts_dir = args.parts_dir.expanduser().resolve()
    args.temp_dir = args.temp_dir.expanduser().resolve() if args.temp_dir else None
    if not args.lake.exists():
        raise SystemExit(f"Lake not found: {args.lake}")

    start = parse_date(args.start)
    end = parse_date(args.end)
    jobs = discover_days(args.lake, source_list(args.source), start, end)
    if not jobs:
        raise SystemExit("No lake day partitions found for requested range/source.")

    log(f"Discovered {len(jobs)} source-day jobs from {jobs[0][1]} to {jobs[-1][1]}")
    processed = skipped = failed = 0
    started = time.time()
    for idx, (source, day_value) in enumerate(jobs, start=1):
        label = f"{source} {day_value.isoformat()}"
        try:
            result = process_day(args, source, day_value)
            if result["status"] == "skipped":
                skipped += 1
            else:
                processed += 1
            percent = idx / len(jobs) * 100
            log(
                f"[{idx}/{len(jobs)} {percent:5.1f}%] {label}: "
                f"{result['status']}, distinct_ua={result['rows']:,}, raw_rows={result['raw_rows']:,}"
            )
        except Exception as exc:
            failed += 1
            log(f"[{idx}/{len(jobs)}] {label}: FAILED: {exc}")
            if not args.dry_run:
                raise

    log(
        f"Daily parts done. processed={processed}, skipped={skipped}, failed={failed}, "
        f"elapsed={time.time() - started:.1f}s"
    )
    if args.dry_run:
        log("Dry-run mode: merge skipped because daily part parquet files were not written.")
        return
    if args.skip_merge:
        return
    merged, out_path = merge_parts(args, jobs)
    log(f"Merged distinct UA profile: {out_path}")
    log(
        f"merged_distinct_ua={len(merged):,}, raw_rows={int(merged['rows'].sum()):,}, "
        f"first_date={merged['first_date'].min()}, last_date={merged['last_date'].max()}"
    )


if __name__ == "__main__":
    main()
