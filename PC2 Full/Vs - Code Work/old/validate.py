import sys
import os
import logging
from datetime import datetime
from pathlib import Path

if os.name == "nt":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

import polars as pl
import config

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
config.LOG_DIR.mkdir(parents=True, exist_ok=True)
log_filename = config.LOG_DIR / f"validate_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(log_filename, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
    force=True,
)
logger = logging.getLogger("validate")

# ─────────────────────────────────────────────
# EXPECTED SCHEMA
# 52 raw + 11 derived = 63 total columns
# ─────────────────────────────────────────────
EXPECTED_COLUMNS = set(config.COLUMNS) | {
    "timestamp", "date", "hour", "minute",
    "is_segment", "is_playlist", "is_error", "cache_hit",
    "quality", "channel", "android_ver",
}

# Critical columns that must not be fully null
CRITICAL_COLUMNS = [
    "cliIP", "reqTimeSec", "reqPath", "reqHost",
    "statusCode", "timestamp", "date", "hour",
    "is_segment", "is_playlist",
]

# Expected Polars dtypes for key derived columns
EXPECTED_TYPES = {
    "is_segment":  pl.Boolean,
    "is_playlist": pl.Boolean,
    "is_error":    pl.Boolean,
    "cache_hit":   pl.Boolean,
    "hour":        pl.Int8,
    "date":        pl.Date,
}


# ─────────────────────────────────────────────
# VALIDATE ONE DAY
# ─────────────────────────────────────────────
def validate_day(day: str) -> dict:
    """
    Runs all health checks on one day's parquet output.
    Returns a result dict with pass/fail per check.
    """
    parquet_dir = config.PARQUET[day]
    result = {
        "day":          day,
        "passed":       True,
        "checks":       {},
        "warnings":     [],
        "errors":       [],
    }

    def fail(check: str, msg: str):
        result["checks"][check] = f"FAIL — {msg}"
        result["errors"].append(msg)
        result["passed"] = False

    def warn(check: str, msg: str):
        result["checks"][check] = f"WARN — {msg}"
        result["warnings"].append(msg)

    def ok(check: str, msg: str):
        result["checks"][check] = f"OK   — {msg}"

    # ── Check 1: Folder exists ────────────────────────────────────────────────
    if not parquet_dir.exists():
        fail("folder_exists", f"{parquet_dir} does not exist")
        return result
    ok("folder_exists", str(parquet_dir))

    # ── Check 2: Date partition subfolders exist ──────────────────────────────
    date_folders = sorted([f for f in parquet_dir.iterdir() if f.is_dir()])
    if not date_folders:
        fail("date_partitions", "No date=YYYY-MM-DD subfolders found")
        return result
    date_names = [f.name for f in date_folders]
    ok("date_partitions", f"{len(date_folders)} partition(s): {date_names}")

    # ── Check 3: Parquet files exist ──────────────────────────────────────────
    parquet_files = list(parquet_dir.rglob("*.parquet"))
    if not parquet_files:
        fail("parquet_files", "No .parquet files found inside partition folders")
        return result
    total_size_mb = sum(f.stat().st_size for f in parquet_files) / (1024 ** 2)
    ok("parquet_files", f"{len(parquet_files)} file(s) | {total_size_mb:.1f} MB total")

    # ── Load lazy frame for remaining checks ──────────────────────────────────
    try:
        lf = pl.scan_parquet(str(parquet_dir / "**/*.parquet"))
    except Exception as e:
        fail("parquet_readable", f"Cannot read parquet: {e}")
        return result
    ok("parquet_readable", "Parquet files opened successfully")

    # ── Check 4: Row count ────────────────────────────────────────────────────
    try:
        row_count = lf.select(pl.len()).collect().item(0, 0)
        if row_count == 0:
            fail("row_count", "Parquet is empty — 0 rows")
        elif row_count < 1000:
            warn("row_count", f"Only {row_count:,} rows — suspiciously low")
        else:
            ok("row_count", f"{row_count:,} rows")
        result["row_count"] = row_count
    except Exception as e:
        fail("row_count", f"Could not count rows: {e}")

    # ── Check 5: Schema — all expected columns present ────────────────────────
    actual_cols   = set(lf.collect_schema().names())
    missing_cols  = EXPECTED_COLUMNS - actual_cols
    extra_cols    = actual_cols - EXPECTED_COLUMNS

    if missing_cols:
        fail("schema_columns", f"Missing columns: {sorted(missing_cols)}")
    else:
        ok("schema_columns", f"All {len(EXPECTED_COLUMNS)} expected columns present")

    if extra_cols:
        warn("extra_columns", f"Extra columns found (ok to ignore): {sorted(extra_cols)}")

    # ── Check 6: Column data types ────────────────────────────────────────────
    schema     = lf.collect_schema()
    type_errors = []
    for col, expected_type in EXPECTED_TYPES.items():
        if col in schema:
            actual_type = schema[col]
            if actual_type != expected_type:
                type_errors.append(f"{col}: expected {expected_type}, got {actual_type}")

    if type_errors:
        warn("column_types", f"Type mismatches: {type_errors}")
    else:
        ok("column_types", "All derived column types correct")

    # ── Check 7: Critical columns null rate ───────────────────────────────────
    try:
        null_exprs = [
            (pl.col(c).is_null().sum() / pl.len() * 100)
            .alias(c)
            for c in CRITICAL_COLUMNS
            if c in actual_cols
        ]
        null_rates = lf.select(null_exprs).collect().to_dicts()[0]

        high_null = {c: f"{v:.1f}%" for c, v in null_rates.items() if v > 50}
        med_null  = {c: f"{v:.1f}%" for c, v in null_rates.items() if 10 < v <= 50}

        if high_null:
            warn("null_rates", f"High nulls (>50%): {high_null}")
        elif med_null:
            warn("null_rates", f"Medium nulls (10-50%): {med_null}")
        else:
            ok("null_rates", "All critical columns have <10% nulls")

        result["null_rates"] = {c: f"{v:.1f}%" for c, v in null_rates.items()}

    except Exception as e:
        warn("null_rates", f"Could not compute null rates: {e}")

    # ── Check 8: Timestamp range ──────────────────────────────────────────────
    try:
        ts_stats = lf.select([
            pl.col("timestamp").min().alias("min_ts"),
            pl.col("timestamp").max().alias("max_ts"),
        ]).collect()

        min_ts = ts_stats.item(0, "min_ts")
        max_ts = ts_stats.item(0, "max_ts")

        if min_ts is None or max_ts is None:
            fail("timestamp_range", "Timestamp column is entirely null")
        else:
            ok("timestamp_range", f"{min_ts} → {max_ts} (IST)")
            result["timestamp_range"] = {"min": str(min_ts), "max": str(max_ts)}

    except Exception as e:
        warn("timestamp_range", f"Could not read timestamps: {e}")

    # ── Check 9: Segment vs playlist ratio ───────────────────────────────────
    try:
        counts = lf.select([
            pl.col("is_segment").sum().alias("segments"),
            pl.col("is_playlist").sum().alias("playlists"),
        ]).collect()

        segments  = counts.item(0, "segments")
        playlists = counts.item(0, "playlists")
        other     = row_count - segments - playlists

        ok("request_types",
           f"segments={segments:,} | playlists={playlists:,} | other={other:,}")

    except Exception as e:
        warn("request_types", f"Could not check request types: {e}")

    # ── Check 10: Unique viewers (sanity) ─────────────────────────────────────
    try:
        unique_ips = lf.select(pl.col("cliIP").n_unique()).collect().item(0, 0)
        if unique_ips == 0:
            warn("unique_viewers", "No unique IPs found")
        else:
            ok("unique_viewers", f"{unique_ips:,} unique viewer IPs")
        result["unique_viewers"] = unique_ips
    except Exception as e:
        warn("unique_viewers", f"Could not count unique IPs: {e}")

    return result


# ─────────────────────────────────────────────
# PRINT REPORT
# ─────────────────────────────────────────────
def print_report(results: list):
    print("\n" + "=" * 65)
    print("  VALIDATION REPORT")
    print("=" * 65)

    all_passed = True
    for r in results:
        day    = r["day"]
        status = "PASS" if r["passed"] else "FAIL"
        emoji  = "OK" if r["passed"] else "!!"
        print(f"\n  [{emoji}] Day {day} — {status}")
        print(f"  {'-' * 50}")

        for check, outcome in r["checks"].items():
            print(f"    {check:<22} {outcome}")

        if r.get("row_count"):
            print(f"\n    Rows            : {r['row_count']:,}")
        if r.get("unique_viewers"):
            print(f"    Unique viewers  : {r['unique_viewers']:,}")
        if r.get("timestamp_range"):
            ts = r["timestamp_range"]
            print(f"    Timestamp range : {ts['min']} → {ts['max']}")
        if r.get("null_rates"):
            print(f"    Null rates      : {r['null_rates']}")
        if r["errors"]:
            print(f"\n    ERRORS:")
            for e in r["errors"]:
                print(f"      - {e}")
        if r["warnings"]:
            print(f"\n    WARNINGS:")
            for w in r["warnings"]:
                print(f"      ~ {w}")

        if not r["passed"]:
            all_passed = False

    print("\n" + "=" * 65)
    if all_passed:
        print("  RESULT: ALL DAYS PASSED — safe to run dashboard")
    else:
        failed = [r["day"] for r in results if not r["passed"]]
        print(f"  RESULT: FAILED days: {failed}")
        print("  Fix errors above, then re-run gz_to_parquet.py --force")
    print("=" * 65 + "\n")

    return all_passed


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Veto Pipeline — Validate parquet output"
    )
    parser.add_argument("--day", type=str, default=None,
                        help="Validate one specific day e.g. --day 01")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("  Veto Pipeline — validate.py")
    logger.info(f"  Started : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    days    = [args.day] if args.day else config.DAY_FOLDERS
    results = []

    for day in days:
        logger.info(f"Validating day {day}...")
        result = validate_day(day)
        results.append(result)

    passed = print_report(results)

    if not passed:
        sys.exit(1)


if __name__ == "__main__":
    main()