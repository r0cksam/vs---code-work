
import sys
import os
import shutil
import logging
import argparse
import time
import tempfile
from datetime import datetime
from pathlib import Path
from typing import List, Optional

# ── Windows UTF-8 fix ─────────────────────────────────────────────────────────
if os.name == "nt":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

import polars as pl
from tqdm import tqdm
import config

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
config.LOG_DIR.mkdir(parents=True, exist_ok=True)
log_filename = config.LOG_DIR / f"gz_to_parquet_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

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
logger = logging.getLogger("gz_to_parquet")

# ─────────────────────────────────────────────
# POLARS STREAMING CONFIG
# ─────────────────────────────────────────────
pl.Config.set_streaming_chunk_size(131_072)

# ─────────────────────────────────────────────
# NUMERIC COLUMNS (cast from string to number)
# ─────────────────────────────────────────────
FLOAT_COLS = {
    "reqTimeSec", "throughput", "timeToFirstByte", "downloadTime",
    "transferTimeMSec", "turnAroundTimeMSec", "tlsOverheadTimeMSec",
    "bytes", "objSize", "totalBytes", "uncompressedSize",
}
INT_COLS = {
    "statusCode", "asn", "reqPort", "streamId", "version",
    "billingRegion", "deliveryPolicyId", "deliveryPolicyReqStatus",
    "deliveryFormat", "deliveryType", "downloadInitiated", "downloadsCompleted",
    "edgeIPBinding", "lastByte", "mediaEncryption", "prefetchMidgressHits",
    "edgeAttempts", "maxAgeSec", "reqEndTimeMSec", "rspContentLen",
}

# ─────────────────────────────────────────────
# RESUME / PROGRESS TRACKING
# ─────────────────────────────────────────────
def load_completed_days() -> set:
    completed = set()
    if config.PROGRESS_LOG.exists():
        with open(config.PROGRESS_LOG, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("DONE:"):
                    completed.add(line.split("DONE:")[1].split("|")[0].strip())
    return completed


def mark_day_complete(day: str) -> None:
    with open(config.PROGRESS_LOG, "a", encoding="utf-8") as f:
        f.write(f"DONE:{day} | {datetime.now().isoformat()}\n")


# ─────────────────────────────────────────────
# POLARS DERIVED COLUMN EXPRESSIONS
# ─────────────────────────────────────────────
def quality_expr() -> pl.Expr:
    return (
        pl.when(pl.col("reqPath").str.contains("1080p")).then(pl.lit("1080p"))
        .when(pl.col("reqPath").str.contains("720p")).then(pl.lit("720p"))
        .when(pl.col("reqPath").str.contains("576p")).then(pl.lit("576p"))
        .when(pl.col("reqPath").str.contains("480p")).then(pl.lit("480p"))
        .when(pl.col("reqPath").str.contains("360p")).then(pl.lit("360p"))
        .when(pl.col("reqPath").str.contains("240p")).then(pl.lit("240p"))
        .when(pl.col("reqPath").str.contains("216p")).then(pl.lit("216p"))
        .otherwise(pl.lit("Auto/Unknown"))
        .alias("quality")
    )


def channel_expr() -> pl.Expr:
    return (
        pl.col("reqHost")
        .str.replace(r"\.akamaized\.net", "")
        .str.replace(r"-veto$", "")
        .alias("channel")
    )


def android_ver_expr() -> pl.Expr:
    return (
        pl.col("UA")
        .str.extract(r"Android[%20\s]+(\d+)", group_index=1)
        .alias("android_ver")
    )


# ─────────────────────────────────────────────
# CORE: TRANSFORM ONE BATCH OF .gz FILES
# ─────────────────────────────────────────────
def transform_batch(batch: List[Path], batch_idx: int, temp_dir: Path) -> Optional[Path]:
    """
    Reads a batch of .gz files and writes a single temp parquet chunk.
    Returns path to chunk on success, None on failure.
    """
    chunk_path = temp_dir / f"chunk_{batch_idx:05d}.parquet"

    try:
        batch_strs = [str(f) for f in batch]

        # Read all .gz files in batch as lazy NDJSON
        # infer_schema_length=None → read all rows to infer schema safely (0 deprecated in newer Polars)
        lf = pl.scan_ndjson(
            batch_strs,
            infer_schema_length=None,
            low_memory=True,
        )

        # Keep only our verified 52 columns (ignore any unknown fields)
        available = lf.collect_schema().names()
        keep = [c for c in config.COLUMNS if c in available]
        lf = lf.select(keep)

        # ── Step 1: Null normalization ────────────────────────────────────────
        # Akamai uses "-" and "^" as null sentinels → replace with actual nulls
        lf = lf.with_columns([
            pl.when(pl.col(pl.Utf8).str.contains(r"^[-^]+$"))
            .then(None)
            .otherwise(pl.col(pl.Utf8))
            .name.keep()
        ])

        # ── Step 2: Cast numeric columns ─────────────────────────────────────
        cast_exprs = []
        for col in keep:
            if col in FLOAT_COLS:
                cast_exprs.append(pl.col(col).cast(pl.Float64, strict=False))
            elif col in INT_COLS:
                cast_exprs.append(pl.col(col).cast(pl.Int64, strict=False))
        if cast_exprs:
            lf = lf.with_columns(cast_exprs)

        # ── Step 3: URL-decode string fields ─────────────────────────────────
        url_cols = [c for c in ["UA", "state", "city", "reqPath"] if c in keep]
        if url_cols:
            lf = lf.with_columns([
                pl.col(c)
                .str.replace_all("%20", " ")
                .str.replace_all("%2F", "/")
                .str.replace_all("%3A", ":")
                .str.replace_all("%2C", ",")
                .alias(c)
                for c in url_cols
            ])

        # ── Step 4: Timestamp → IST ───────────────────────────────────────────
        lf = (
            lf
            .with_columns([
                (pl.col("reqTimeSec") * 1000)
                .cast(pl.Int64, strict=False)
                .alias("epoch_ms")
            ])
            .with_columns([
                pl.from_epoch(pl.col("epoch_ms"), time_unit="ms")
                .dt.convert_time_zone(config.TIMEZONE)
                .alias("timestamp")
            ])
            .drop("epoch_ms")
        )

        # ── Step 5: Derived business columns ─────────────────────────────────
        lf = lf.with_columns([
            pl.col("timestamp").dt.date().alias("date"),       # ← partition key
            pl.col("timestamp").dt.hour().alias("hour"),
            pl.col("timestamp").dt.minute().alias("minute"),
            pl.col("reqPath").str.contains(r"\.ts").alias("is_segment"),
            pl.col("reqPath").str.contains(r"\.m3u8").alias("is_playlist"),
            (pl.col("statusCode").cast(pl.Int64, strict=False) >= 400).alias("is_error"),
            (pl.col("cacheStatus") == "1").alias("cache_hit"),
            quality_expr(),
            channel_expr(),
            android_ver_expr(),
        ])

        # ── Write temp chunk ──────────────────────────────────────────────────
        lf.sink_parquet(
            str(chunk_path),
            compression="zstd",
            compression_level=5,
            statistics=True,
            row_group_size=2_000_000,
        )
        return chunk_path

    except Exception as e:
        logger.error(f"Batch {batch_idx:05d} failed: {e}", exc_info=True)
        if chunk_path.exists():
            chunk_path.unlink(missing_ok=True)
        return None


# ─────────────────────────────────────────────
# PROCESS ONE COMPLETE DAY FOLDER
# ─────────────────────────────────────────────
def process_day(day: str, force: bool = False) -> bool:
    raw_dir     = config.RAW[day]
    parquet_dir = config.PARQUET[day]
    archive_dir = config.ARCHIVE_DIR / day

    # Validate
    if not raw_dir.exists():
        logger.warning(f"Day {day}: raw folder not found at {raw_dir} — skipping")
        return False

    gz_files = sorted(raw_dir.glob("*.gz"))
    if not gz_files:
        logger.warning(f"Day {day}: no .gz files found — skipping")
        return False

    logger.info(f"Day {day}: {len(gz_files):,} .gz files found")

    # Create output folder
    parquet_dir.mkdir(parents=True, exist_ok=True)

    # Split into batches
    batches   = [gz_files[i:i + config.BATCH_SIZE]
                 for i in range(0, len(gz_files), config.BATCH_SIZE)]
    temp_dir  = Path(tempfile.mkdtemp(prefix=f"veto_{day}_"))
    chunks    = []
    start     = time.time()

    logger.info(f"Day {day}: {len(batches)} batches × {config.BATCH_SIZE} files each")

    try:
        # ── Process batches ───────────────────────────────────────────────────
        for i, batch in enumerate(tqdm(batches, desc=f"Day {day}", unit="batch")):
            result = transform_batch(batch, i, temp_dir)
            if result:
                chunks.append(result)
            else:
                logger.warning(f"Day {day}: batch {i} failed — continuing with rest")

        if not chunks:
            logger.error(f"Day {day}: all batches failed — nothing written")
            return False

        # ── Merge all chunks & write partitioned parquet ──────────────────────
        logger.info(f"Day {day}: merging {len(chunks)} chunks → partitioned by date")

        merged = pl.scan_parquet([str(c) for c in chunks])

        # Write partitioned by date — creates subfolders automatically:
        # 01_parquet/date=2026-04-01/part-0.parquet
        # 01_parquet/date=2026-04-02/part-0.parquet
        merged.sink_parquet(
            pl.PartitionBy(
                str(parquet_dir),
                key=[config.PARTITION_BY],
            ),
            compression="zstd",
            compression_level=5,
            statistics=True,
            row_group_size=2_000_000,
            mkdir=True,
        )

        # ── Stats ─────────────────────────────────────────────────────────────
        total_rows = (
            pl.scan_parquet(str(parquet_dir / "**/*.parquet"))
            .select(pl.len())
            .collect()
            .item(0, 0)
        )
        total_size_mb = sum(
            f.stat().st_size for f in parquet_dir.rglob("*.parquet")
        ) / (1024 ** 2)

        date_folders = sorted([f.name for f in parquet_dir.iterdir() if f.is_dir()])
        duration = time.time() - start

        logger.info(f"Day {day}: SUCCESS")
        logger.info(f"  Total rows   : {total_rows:,}")
        logger.info(f"  Total size   : {total_size_mb:.1f} MB")
        logger.info(f"  Duration     : {duration:.1f}s")
        logger.info(f"  Date folders : {date_folders}")

        # ── Archive source .gz files ──────────────────────────────────────────
        logger.info(f"Day {day}: archiving source files → {archive_dir}")
        archive_dir.mkdir(parents=True, exist_ok=True)

        for gz in tqdm(gz_files, desc=f"Archiving {day}", unit="file", leave=False):
            try:
                shutil.move(str(gz), str(archive_dir / gz.name))
            except Exception as e:
                logger.warning(f"Could not archive {gz.name}: {e}")

        mark_day_complete(day)
        return True

    except Exception as e:
        logger.error(f"Day {day}: unexpected error — {e}", exc_info=True)
        return False

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
        logger.debug(f"Day {day}: temp directory cleaned up")


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Veto Pipeline — Convert .gz logs to date-partitioned Parquet"
    )
    parser.add_argument("--day",   type=str, default=None,
                        help="Process one specific day e.g. --day 01")
    parser.add_argument("--force", action="store_true",
                        help="Reprocess even if day is already marked complete")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("  Veto Pipeline — gz_to_parquet.py")
    logger.info(f"  Started : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"  Log     : {log_filename}")
    logger.info("=" * 60)

    days      = [args.day] if args.day else config.DAY_FOLDERS
    completed = load_completed_days() if not args.force else set()

    if completed:
        logger.info(f"Already completed (skipping): {sorted(completed)}")

    pending = [d for d in days if d not in completed]
    if not pending:
        logger.info("All days already processed. Use --force to reprocess.")
        return

    logger.info(f"Days to process: {pending}")

    results = {}
    for day in pending:
        logger.info(f"\n{'─' * 50}")
        logger.info(f"  Processing day: {day}")
        logger.info(f"{'─' * 50}")
        results[day] = process_day(day, force=args.force)

    # ── Final summary ─────────────────────────────────────────────────────────
    logger.info(f"\n{'=' * 60}")
    logger.info("  SUMMARY")
    logger.info(f"{'=' * 60}")
    for day, ok in results.items():
        logger.info(f"  Day {day}: {'SUCCESS' if ok else 'FAILED'}")

    failed = [d for d, ok in results.items() if not ok]
    if failed:
        logger.warning(f"Failed days: {failed} — re-run with --force to retry")
        sys.exit(1)
    else:
        logger.info("All days completed successfully.")


if __name__ == "__main__":
    main()