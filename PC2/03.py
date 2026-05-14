import duckdb
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from tqdm import tqdm

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────

BASE_FOLDER  = Path(r"Z:\05 Veto Logs")
LAKE_FOLDER  = BASE_FOLDER / "lake"          # output: lake/year=YYYY/month=MM/day=DD/
LOG_FILE     = BASE_FOLDER / "03_processed_log.json"

THREADS      = 12
MEMORY       = "28GB"
COMPRESSION  = "ZSTD"
COMP_LEVEL   = 3                             # 3 is fast; raise to 9+ for colder data


# ─────────────────────────────────────────────
# Log helpers  (save after every file so a crash = safe resume)
# ─────────────────────────────────────────────

def load_log() -> dict:
    if LOG_FILE.exists():
        with LOG_FILE.open("r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_log(log: dict):
    with LOG_FILE.open("w", encoding="utf-8") as f:
        json.dump(log, f, indent=2)


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    LAKE_FOLDER.mkdir(parents=True, exist_ok=True)

    log = load_log()

    # Discover all final_clean files, sorted by their numeric prefix
    all_files = sorted(
        BASE_FOLDER.glob("*_final_clean.parquet"),
        key=lambda p: int(p.name.split("_")[0])
    )

    new_files = [f for f in all_files if f.name not in log]

    print(f"Total final_clean files : {len(all_files)}")
    print(f"Already processed       : {len(all_files) - len(new_files)}")
    print(f"New to process          : {len(new_files)}")

    if not new_files:
        print("\n✅  Nothing to do — lake is up to date.")
        return

    con = duckdb.connect()
    con.execute(f"SET threads={THREADS};")
    con.execute(f"SET memory_limit='{MEMORY}';")
    con.execute("SET preserve_insertion_order=false;")
    con.execute("SET enable_progress_bar=true;")
    con.execute("SET enable_progress_bar_print=true;")

    bar = tqdm(
        new_files,
        unit="file",
        desc="  Partitioning",
        dynamic_ncols=True,
        bar_format=(
            "{desc}: {percentage:3.0f}%|{bar}| "
            "{n_fmt}/{total_fmt} files "
            "[{elapsed}<{remaining}]"
        ),
    )

    for f in bar:
        # e.g.  "01_final_clean.parquet"  →  src_prefix = "src01"
        num_part   = f.name.split("_")[0]          # "01", "02", ...
        src_prefix = f"src{num_part}"               # used in output filenames

        print(f"\n{'─'*60}")
        print(f"🚀  Processing : {f.name}")
        print(f"    Lake root  : {LAKE_FOLDER}")
        print(f"    Filename   : {src_prefix}_{{i}}.parquet  (per partition)")

        start = time.time()

        try:
            # ── Row count (quick metadata read, no full scan) ──────────
            rows = con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{f.as_posix()}')"
            ).fetchone()[0]

            # ── Partition & write ──────────────────────────────────────
            # to_timestamp() converts float seconds → TIMESTAMP
            # strftime extracts year/month/day as zero-padded strings
            # FILENAME_PATTERN guarantees no collision across source files
            #   e.g. lake/year=2026/month=04/day=29/src01_0.parquet
            con.execute(f"""
                COPY (
                    SELECT
                        *,
                        strftime(to_timestamp(CAST(reqTimeSec AS DOUBLE)), '%Y')  AS year,
                        strftime(to_timestamp(CAST(reqTimeSec AS DOUBLE)), '%m')  AS month,
                        strftime(to_timestamp(CAST(reqTimeSec AS DOUBLE)), '%d')  AS day
                    FROM read_parquet('{f.as_posix()}')
                )
                TO '{LAKE_FOLDER.as_posix()}'
                (
                    FORMAT          PARQUET,
                    PARTITION_BY    (year, month, day),
                    FILENAME_PATTERN '{src_prefix}_{{i}}',
                    COMPRESSION     {COMPRESSION},
                    COMPRESSION_LEVEL {COMP_LEVEL},
                    OVERWRITE_OR_IGNORE true
                );
            """)

            elapsed = time.time() - start

            # ── Log success immediately ────────────────────────────────
            log[f.name] = {
                "status"       : "ok",
                "processed_at" : datetime.now(timezone.utc).isoformat(),
                "rows"         : rows,
                "elapsed_sec"  : round(elapsed, 2),
                "src_prefix"   : src_prefix,
            }
            save_log(log)

            bar.set_postfix({"rows": f"{rows:,}", "last": f.name}, refresh=True)
            print(f"✅  Done in {elapsed / 60:.1f} min — {rows:,} rows")

        except Exception as e:
            elapsed = time.time() - start

            # Log failure (but do NOT mark as ok — will retry next run)
            log[f.name + "__error"] = {
                "status"       : "error",
                "processed_at" : datetime.now(timezone.utc).isoformat(),
                "file"         : f.name,
                "error"        : str(e),
                "elapsed_sec"  : round(elapsed, 2),
            }
            save_log(log)

            print(f"❌  Error on {f.name}: {e}")
            # Continue to next file — don't abort the whole run

    bar.close()
    con.close()

    # ── Summary ───────────────────────────────────────────────────────
    final_log = load_log()
    ok_entries = [v for v in final_log.values() if isinstance(v, dict) and v.get("status") == "ok"]
    print(f"\n{'='*60}")
    print(f"🎯  Lake partitioning complete.")
    print(f"    Total processed (all time) : {len(ok_entries)} file(s)")
    print(f"    Lake location              : {LAKE_FOLDER}")
    print(f"    Log                        : {LOG_FILE}")
    print(f"{'='*60}\n")

    # ── Print date partition summary ──────────────────────────────────
    print("📂  Lake partition tree (top 3 levels):")
    for year_dir in sorted(LAKE_FOLDER.iterdir()):
        if not year_dir.is_dir():
            continue
        for month_dir in sorted(year_dir.iterdir()):
            if not month_dir.is_dir():
                continue
            day_count  = sum(1 for d in month_dir.iterdir() if d.is_dir())
            file_count = sum(len(list(d.glob("*.parquet"))) for d in month_dir.iterdir() if d.is_dir())
            print(f"    {year_dir.name}/{month_dir.name}/  →  {day_count} day(s), {file_count} parquet file(s)")


if __name__ == "__main__":
    main()