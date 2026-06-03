import os
import sys
import csv
import gzip
import hashlib
import json
import time
import platform
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

import orjson
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Defaults
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
_CPUS               = os.cpu_count() or 4
DEFAULT_WORKERS     = min(max(1, _CPUS - 1), 32)
DEFAULT_BATCH_SIZE  = 10000
DEFAULT_COMPRESSION = "zstd"
DEFAULT_COMPRESSION_LEVEL = int(os.getenv("VG_ETL_001_COMPRESSION_LEVEL", "3"))
DEFAULT_ROW_BUFFER  = 100_000
DEFAULT_ROW_GROUP   = 200_000
PLACEHOLDERS        = {"-", "^"}
CONVERSION_VERSION  = 4

ETL_ROOT            = Path(__file__).resolve().parents[2]
PORTABLE_PREFS_FILE = ETL_ROOT / "output" / "state" / "gz_parquet_prefs.json"
LEGACY_PREFS_FILE   = Path.home() / ".gz_parquet_prefs.json"
COLUMN_SAMPLE_SIZE  = 1          # how many .gz files to scan for column discovery
_PREFS_NOTICE_PRINTED = False


def resolve_prefs_file() -> Path:
    env_path = os.getenv("VG_ETL_001_PREFS_FILE")
    if env_path:
        return Path(env_path).expanduser()
    if PORTABLE_PREFS_FILE.exists():
        return PORTABLE_PREFS_FILE
    if LEGACY_PREFS_FILE.exists():
        return LEGACY_PREFS_FILE
    return PORTABLE_PREFS_FILE


PREFS_FILE = resolve_prefs_file()


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Pretty helpers
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def clear():
    os.system("cls" if platform.system() == "Windows" else "clear")

def banner():
    print("=" * 60)
    print("   GZ  â†’  PARQUET  Converter")
    print("   Fast batch converter for millions of .gz JSONL files")
    print("=" * 60)
    print()

def ask(prompt: str, default: str = "") -> str:
    hint = f"  [{default}]" if default else ""
    try:
        val = input(f"  {prompt}{hint}: ").strip()
    except (EOFError, KeyboardInterrupt):
        print("\n\nCancelled.")
        sys.exit(0)
    return val if val else default

def ask_int(prompt: str, default: int) -> int:
    while True:
        raw = ask(prompt, str(default))
        try:
            return int(raw)
        except ValueError:
            print(f"  âš   Please enter a whole number.\n")

def ask_choice(prompt: str, choices: list[str], default: str) -> str:
    choices_str = " / ".join(
        f"[{c}]" if c == default else c for c in choices
    )
    while True:
        raw = ask(f"{prompt}  ({choices_str})", default)
        if raw in choices:
            return raw
        print(f"  âš   Choose one of: {', '.join(choices)}\n")

def ask_yn(prompt: str, default: bool) -> bool:
    hint = "Y/n" if default else "y/N"
    raw  = ask(f"{prompt}  ({hint})", "y" if default else "n").lower()
    return raw in ("y", "yes")

def section(title: str):
    print()
    print(f"  â”€â”€ {title} " + "â”€" * max(0, 48 - len(title)))


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Saved column preferences
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def load_prefs() -> dict | None:
    """Load saved column prefs. Returns None if not found."""
    global _PREFS_NOTICE_PRINTED
    if not _PREFS_NOTICE_PRINTED:
        status = "found" if PREFS_FILE.exists() else "not found"
        print(f"  Column prefs   : {PREFS_FILE} ({status})")
        _PREFS_NOTICE_PRINTED = True
    try:
        if PREFS_FILE.exists():
            with PREFS_FILE.open("r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as exc:
        print(f"  Warning: could not read column prefs {PREFS_FILE}: {exc}")
    return None


def save_prefs(cols_to_keep: list[str], cols_to_drop: list[str]):
    """Save column selection for future runs."""
    data = {
        "saved_at":    datetime.now(timezone.utc).isoformat(),
        "cols_to_keep": cols_to_keep,
        "cols_to_drop": cols_to_drop,
    }
    try:
        PREFS_FILE.parent.mkdir(parents=True, exist_ok=True)
        with PREFS_FILE.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        print(f"\n  âœ“  Preferences saved â†’ {PREFS_FILE}")
    except Exception as e:
        print(f"\n  âš   Could not save prefs: {e}")


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Column discovery
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def discover_columns(input_dir: Path, sample_size: int = COLUMN_SAMPLE_SIZE) -> list[str]:
    """
    Scan up to `sample_size` .gz files and collect all unique top-level keys.
    Returns a sorted list of column names.
    """
    sample = list(islice(input_dir.rglob("*.gz"), sample_size))

    seen: set[str] = set()
    for fpath in sample:
        try:
            with gzip.open(fpath, "rb") as fh:
                for raw in fh:
                    raw = raw.strip()
                    if not raw:
                        continue
                    try:
                        obj = orjson.loads(raw)
                        if isinstance(obj, dict):
                            seen.update(obj.keys())
                    except Exception:
                        continue
                    if len(seen) > 5000:          # safety cap â€” stop scanning early
                        break
        except Exception:
            continue

    return sorted(seen)



def discover_columns_from_folders(input_dirs: list[Path], sample_size_per_folder: int = COLUMN_SAMPLE_SIZE) -> list[str]:
    """
    Discover columns across multiple child folders.
    Scans up to sample_size_per_folder .gz files from each folder.
    """
    seen: set[str] = set()
    for input_dir in input_dirs:
        sample = list(islice(input_dir.rglob("*.gz"), sample_size_per_folder))
        for fpath in sample:
            try:
                with gzip.open(fpath, "rb") as fh:
                    for raw in fh:
                        raw = raw.strip()
                        if not raw:
                            continue
                        try:
                            obj = orjson.loads(raw)
                            if isinstance(obj, dict):
                                seen.update(obj.keys())
                        except Exception:
                            continue
                        if len(seen) > 5000:
                            return sorted(seen)
            except Exception:
                continue
    return sorted(seen)


def _has_gz_fast(folder: Path) -> bool:
    """Return True as soon as one .gz file is found. Safer for huge Windows folders."""
    try:
        for root, dirs, files in os.walk(folder):
            # Do not waste time scanning parquet output folders if they are nested by mistake.
            dirs[:] = [
                d for d in dirs
                if not d.lower().endswith(("_parquet", "_parqut"))
            ]
            for filename in files:
                if filename.lower().endswith(".gz"):
                    return True
    except PermissionError:
        return False
    except OSError:
        return False
    return False


def find_child_input_folders(master_dir: Path) -> list[Path]:
    """
    Find direct child folders inside master_dir that contain .gz files.
    Skips already-created output folders like 01_parquet / 01_parqut.
    Prints progress so the script never looks frozen on huge folders.
    """
    children = []

    direct_children = [c for c in sorted(master_dir.iterdir()) if c.is_dir()]
    direct_children = [
        c for c in direct_children
        if not c.name.lower().endswith(("_parquet", "_parqut"))
    ]

    print(f"\n  Checking {len(direct_children)} child folder(s) for .gz files ...", flush=True)

    for i, child in enumerate(direct_children, 1):
        print(f"  [{i}/{len(direct_children)}] Checking: {child.name} ...", end=" ", flush=True)
        if _has_gz_fast(child):
            print("found", flush=True)
            children.append(child)
        else:
            print("no .gz", flush=True)

    return children


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Column selection menu
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _print_columns(columns: list[str], dropped: set[str]):
    """Pretty-print numbered column list with keep/drop status."""
    print()
    print(f"  {'#':<5}  {'STATUS':<8}  COLUMN")
    print(f"  {'â”€'*5}  {'â”€'*8}  {'â”€'*40}")
    for i, col in enumerate(columns, 1):
        status = "DROP  âœ—" if col in dropped else "keep  âœ“"
        print(f"  {i:<5}  {status:<8}  {col}")
    total_keep = len(columns) - len(dropped)
    print(f"\n  Keeping {total_keep} of {len(columns)} columns  |  Dropping {len(dropped)}")


def _parse_numbers(raw: str, max_val: int) -> list[int]:
    """Parse a comma/space separated list of integers from user input."""
    result = []
    for part in raw.replace(",", " ").split():
        try:
            n = int(part)
            if 1 <= n <= max_val:
                result.append(n)
        except ValueError:
            pass
    return result


def column_selection_menu(columns: list[str]) -> list[str]:
    """
    Interactive column picker. Returns the list of columns to KEEP.
    """
    if not columns:
        print("  âš   No columns discovered. All data will be kept as-is.")
        return []

    dropped: set[str] = set()

    print()
    print(f"  Found {len(columns)} unique columns across sampled files.")
    _print_columns(columns, dropped)

    print()
    print("  How to select:")
    print("    â€¢ Type column numbers to DROP  (e.g.  3 7 12)")
    print("    â€¢ Type  'reset'  to restore all columns")
    print("    â€¢ Type  'done'   (or press Enter) when finished")
    print()

    while True:
        try:
            raw = input("  > ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\n\nCancelled.")
            sys.exit(0)

        if raw in ("", "done"):
            break

        if raw == "reset":
            dropped.clear()
            _print_columns(columns, dropped)
            continue

        nums = _parse_numbers(raw, len(columns))
        if not nums:
            print("  âš   Enter column numbers separated by spaces or commas.\n")
            continue

        for n in nums:
            col = columns[n - 1]
            if col in dropped:
                dropped.discard(col)          # toggle back on
                print(f"  âœ“  Restored  â†’ {col}")
            else:
                dropped.add(col)
                print(f"  âœ—  Dropping  â†’ {col}")

        _print_columns(columns, dropped)

    cols_to_keep = [c for c in columns if c not in dropped]
    cols_to_drop = [c for c in columns if c in dropped]
    return cols_to_keep, cols_to_drop


def _offer_saved_prefs(saved: dict, discovered: list[str]) -> tuple[list[str], list[str]] | None:
    """
    Show the user their saved column prefs and ask whether to reuse them.
    Returns (cols_to_keep, cols_to_drop) or None if user wants to reconfigure.
    """
    saved_keep = saved.get("cols_to_keep", [])
    saved_drop = saved.get("cols_to_drop", [])
    saved_at   = saved.get("saved_at", "unknown")

    new_cols = [c for c in discovered if c not in saved_keep and c not in saved_drop]
    missing_keep = [c for c in saved_keep if c not in discovered]

    print(f"\n  â”Œâ”€  Saved column preferences found  (saved: {saved_at[:19]})")
    print(f"  â”‚   Keeping  : {len(saved_keep)} columns")
    print(f"  â”‚   Dropping : {len(saved_drop)} columns")
    if missing_keep:
        print(f"  â”‚   Kept cols not seen in sample : {len(missing_keep)} (still kept in output schema)")
    if new_cols:
        print(f"  â”‚   NEW cols not in saved prefs : {len(new_cols)}")
        for c in new_cols[:10]:
            print(f"  â”‚     + {c}")
        if len(new_cols) > 10:
            print(f"  â”‚     ... and {len(new_cols)-10} more")
    print(f"  â””{'â”€'*50}")

    use_saved = ask_yn("  Use saved preferences?", True)
    if use_saved:
        # New columns default to keep â€” user can override in full menu if they want
        all_keep = saved_keep + new_cols
        return all_keep, saved_drop

    return None          # signal: go to full selection menu

 
def _reuse_saved_prefs_non_interactive(
    saved: dict | None,
    discovered: list[str],
) -> tuple[list[str], list[str]]:
    if not saved:
        return [], []
    saved_keep = list(dict.fromkeys(saved.get("cols_to_keep") or []))
    saved_drop = list(dict.fromkeys(saved.get("cols_to_drop") or []))
    new_cols = [c for c in discovered if c not in saved_keep and c not in saved_drop]
    missing_keep = [c for c in saved_keep if c not in discovered]
    if missing_keep:
        print(f"  Saved keep columns not seen in sample: {len(missing_keep)} (still kept in output schema)")
    return saved_keep + new_cols, saved_drop
# Interactive menu
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def interactive_menu() -> dict:
    clear()
    banner()

    # â”€â”€ Input folder â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    section("INPUT")
    print("  Enter the folder that contains your .gz files.")
    print("  Subfolders are included automatically.\n")
    while True:
        input_dir = Path(ask("Input folder path")).expanduser().resolve()
        if input_dir.exists() and input_dir.is_dir():
            break
        print(f"  âš   Folder not found: {input_dir}\n")

    # â”€â”€ Output folder â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    section("OUTPUT")
    default_out = str(input_dir.parent / (input_dir.name + "_parquet"))
    print(f"  Where should Parquet files be saved?\n")
    output_dir = Path(ask("Output folder path", default_out)).expanduser().resolve()

    # â”€â”€ Column discovery & selection â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    section("COLUMN SELECTION")
    print(f"  Scanning up to {COLUMN_SAMPLE_SIZE} files to discover columns ...\n")
    all_columns = discover_columns(input_dir)

    if not all_columns:
        print("  âš   Could not discover any columns. Proceeding with all data.\n")
        cols_to_keep, cols_to_drop = [], []
    else:
        saved = load_prefs()
        cols_to_keep = None

        if saved:
            cols_to_keep, cols_to_drop = _reuse_saved_prefs_non_interactive(saved, all_columns)

        if cols_to_keep is None:
            # Full interactive selection
            cols_to_keep, cols_to_drop = column_selection_menu(all_columns)

    # â”€â”€ Workers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    section("PERFORMANCE")
    print(f"  Your machine has {_CPUS} CPU cores.")
    print(f"  More workers = faster, but uses more RAM.\n")
    workers = ask_int(f"Worker processes", DEFAULT_WORKERS)

    # â”€â”€ Batch size â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print()
    print("  Batch size = how many .gz files go into one Parquet file.")
    print("  Larger = fewer output files, slightly more RAM per worker.\n")
    batch_size = ask_int("Batch size (files per Parquet)", DEFAULT_BATCH_SIZE)

    # â”€â”€ Compression â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    section("COMPRESSION")
    print("  zstd  â†’ best size, good speed  (recommended)")
    print("  snappyâ†’ fastest, larger files")
    print("  lz4   â†’ balanced")
    print("  none  â†’ no compression (largest files, fastest write)\n")
    compression = ask_choice("Compression codec",
                             ["zstd", "snappy", "lz4", "gzip", "brotli", "none"],
                             "zstd")

    # â”€â”€ Options â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    section("OPTIONS")
    add_meta = ask_yn("Add '_src_file' column (source filename in each row)?", False)
    # Smart resume is always ON: completed parquet batches are skipped automatically.
    resume   = True

    # â”€â”€ Confirm â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    section("READY TO START")
    print(f"  Input folder  : {input_dir}")
    print(f"  Output folder : {output_dir}")
    print(f"  Workers       : {workers}")
    print(f"  Batch size    : {batch_size} files/parquet")
    print(f"  Compression   : {compression}")
    print(f"  Add src meta  : {add_meta}")
    print("  Smart skip     : ON -- already-converted batches will be skipped automatically")
    if cols_to_keep:
        print(f"  Columns kept  : {len(cols_to_keep)}")
        print(f"  Columns drop  : {len(cols_to_drop)}")
    else:
        print(f"  Columns       : ALL (no filter)")
    print()

    go = ask_yn("Start conversion?", True)
    if not go:
        print("\n  Cancelled.\n")
        sys.exit(0)

    return {
        "input_dir":    input_dir,
        "output_dir":   output_dir,
        "workers":      workers,
        "batch_size":   batch_size,
        "compression":  compression,
        "add_meta":     add_meta,
        "resume":       resume,
        "cols_to_keep": cols_to_keep,   # empty list = keep everything
        "cols_to_drop": cols_to_drop,
    }


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Worker (subprocess) â€” no I/O to terminal
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _convert_batch(task: dict) -> dict:
    out_path  = Path(task["out_path"])
    tmp_path  = out_path.with_suffix(".parquet.tmp")
    files     = task["files"]
    compress  = task["compression"]
    buf_size  = task["row_buffer"]
    rg_size   = task["row_group_size"]
    add_meta  = task["add_meta"]
    null_set  = set(task["placeholders"])
    keep_cols = list(dict.fromkeys(task["cols_to_keep"]))
    drop_set  = set(task.get("cols_to_drop") or [])
    keep_set  = set(keep_cols)   # empty = keep all
    filter_cols = bool(keep_set)
    compression_arg = None if str(compress).lower() == "none" else compress

    rows_written = 0
    files_ok     = 0
    failed_files = []
    writer       = None
    schema       = None
    buffer       = []

    def _flush(buf):
        nonlocal writer, schema, rows_written

        if not buf:
            return

        if schema is None:
            if filter_cols:
                all_cols = list(keep_cols)
                if add_meta and "_src_file" not in all_cols:
                    all_cols.append("_src_file")
            else:
                all_cols = sorted({k for row in buf for k in row.keys()})
            schema = pa.schema([pa.field(c, pa.string()) for c in all_cols])

        cols = schema.names
        normalized = [{c: row.get(c) for c in cols} for row in buf]

        tbl = pa.Table.from_pylist(normalized, schema=schema)

        if writer is None:
            writer = pq.ParquetWriter(
                str(tmp_path),
                schema,
                compression=compression_arg,
                compression_level=DEFAULT_COMPRESSION_LEVEL if compression_arg == "zstd" else None
            )

        writer.write_table(tbl, row_group_size=rg_size)
        rows_written += len(buf)

    try:
        for fpath in files:
            try:
                with gzip.open(fpath, "rb") as fh:
                    for raw in fh:
                        raw = raw.strip()
                        if not raw:
                            continue
                        try:
                            obj = orjson.loads(raw)
                        except Exception as je:
                            failed_files.append({"file": fpath, "error": f"JSON: {je}", "stage": "parse"})
                            continue
                        if not isinstance(obj, dict):
                            failed_files.append({"file": fpath, "error": "Not a JSON object", "stage": "parse"})
                            continue

                        row = {}
                        for k, v in obj.items():
                            # â”€â”€ column filter â”€â”€
                            if k in drop_set or (filter_cols and k not in keep_set):
                                continue
                            if v is None:
                                row[k] = None
                            elif isinstance(v, (dict, list)):
                                row[k] = orjson.dumps(v).decode()
                            else:
                                s = str(v)
                                row[k] = None if s in null_set else s

                        if add_meta:
                            row["_src_file"] = os.path.basename(fpath)

                        buffer.append(row)
                        if len(buffer) >= buf_size:
                            _flush(buffer)
                            buffer.clear()

                files_ok += 1
            except Exception as e:
                failed_files.append({"file": fpath, "error": str(e), "stage": "read"})

        _flush(buffer)

        if writer is None:
            return {"status": "empty", "out": str(out_path),
                    "rows": 0, "files_ok": files_ok, "failed_files": failed_files}

        writer.close()
        tmp_path.replace(out_path)
        return {"status": "ok", "out": out_path.name,
                "rows": rows_written, "files_ok": files_ok, "failed_files": failed_files}

    except Exception as e:
        try:
            if writer:
                writer.close()
        except Exception:
            pass
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        return {"status": "error", "out": str(out_path),
                "rows": rows_written, "files_ok": files_ok,
                "failed_files": failed_files, "error": str(e)}


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Error log
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

class ErrorLog:
    FIELDS = ["timestamp", "batch", "stage", "file", "error"]

    def __init__(self, path: Path):
        self._fh    = path.open("w", newline="", encoding="utf-8")
        self._csv   = csv.DictWriter(self._fh, fieldnames=self.FIELDS)
        self._csv.writeheader()
        self._count = 0
        self.path   = path

    def write_batch_error(self, bid, result):
        self._csv.writerow({"timestamp": datetime.now(timezone.utc).isoformat(),
                            "batch": bid, "stage": "batch",
                            "file": result.get("out", ""),
                            "error": result.get("error", "")})
        self._count += 1
        self._fh.flush()

    def write_file_errors(self, bid, items):
        ts = datetime.now(timezone.utc).isoformat()
        for it in items:
            self._csv.writerow({"timestamp": ts, "batch": bid,
                                "stage": it.get("stage", ""), "file": it.get("file", ""),
                                "error": it.get("error", "")})
            self._count += 1
        if items:
            self._fh.flush()

    def close(self):
        self._fh.close()

    @property
    def count(self):
        return self._count


def _batch_manifest_path(output_dir: Path) -> Path:
    return output_dir / "_batch_manifest.json"


def load_batch_manifest(output_dir: Path) -> dict:
    path = _batch_manifest_path(output_dir)
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else {}
    except Exception as exc:
        print(f"  Warning: could not read batch manifest {path}: {exc}")
        return {}


def save_batch_manifest(output_dir: Path, manifest: dict) -> None:
    path = _batch_manifest_path(output_dir)
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
    tmp.replace(path)


def batch_signature(files: list[str], input_dir: Path) -> str:
    root = Path(input_dir)
    h = hashlib.blake2b(digest_size=16)
    for item in files:
        p = Path(item)
        try:
            rel = p.relative_to(root).as_posix()
        except ValueError:
            rel = str(p)
        stat = p.stat()
        h.update(f"{rel}|{stat.st_size}|{stat.st_mtime_ns}\n".encode("utf-8", errors="replace"))
    return h.hexdigest()


def conversion_signature(cfg: dict) -> str:
    payload = {
        "version": CONVERSION_VERSION,
        "compression": cfg.get("compression"),
        "compression_level": DEFAULT_COMPRESSION_LEVEL,
        "add_meta": bool(cfg.get("add_meta")),
        "cols_to_keep": list(cfg.get("cols_to_keep") or []),
        "cols_to_drop": list(cfg.get("cols_to_drop") or []),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.blake2b(raw.encode("utf-8"), digest_size=16).hexdigest()


def prune_stale_batches(output_dir: Path, current_batch_ids: set[str], manifest: dict) -> int:
    removed = 0
    for out in output_dir.glob("part_*.parquet"):
        stem = out.stem
        if not stem.startswith("part_"):
            continue
        suffix = stem[len("part_"):]
        if not suffix.isdigit():
            continue
        batch_id = f"batch_{suffix}"
        if batch_id in current_batch_ids:
            continue
        try:
            out.unlink()
            removed += 1
        except FileNotFoundError:
            pass

    for batch_id in list(manifest):
        if batch_id not in current_batch_ids:
            manifest.pop(batch_id, None)
    return removed


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Pipeline
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def run(cfg: dict):
    input_dir  = cfg["input_dir"]
    output_dir = cfg["output_dir"]
    output_dir.mkdir(parents=True, exist_ok=True)

    print()
    print(f"  Scanning {input_dir} ...")
    t0    = time.time()
    paths = [str(p) for p in sorted(input_dir.rglob("*.gz"))]   # sorted for stable batch IDs
    print(f"  Found {len(paths):,} .gz files  ({time.time()-t0:.1f}s)")

    if not paths:
        print("\n  No .gz files found. Check your input folder.\n")
        return {"status": "no_files", "output_dir": str(output_dir), "rows": 0, "errors": 0}

    bs      = cfg["batch_size"]
    batches = [paths[i:i+bs] for i in range(0, len(paths), bs)]
    manifest = load_batch_manifest(output_dir)
    conv_sig = conversion_signature(cfg)
    current_batch_ids = {f"batch_{idx:07d}" for idx in range(len(batches))}
    stale_removed = prune_stale_batches(output_dir, current_batch_ids, manifest)
    if stale_removed:
        print(f"  Removed stale batch parquet files: {stale_removed:,}")
        save_batch_manifest(output_dir, manifest)

    tasks   = []
    skipped = 0
    for idx, chunk in enumerate(batches):
        batch_id = f"batch_{idx:07d}"
        out = output_dir / f"part_{idx:07d}.parquet"
        sig = batch_signature(chunk, input_dir)
        rec = manifest.get(batch_id, {})
        can_skip = (
            cfg["resume"]
            and out.exists()
            and out.stat().st_size > 0
            and isinstance(rec, dict)
            and rec.get("signature") == sig
            and rec.get("conversion_signature") == conv_sig
            and rec.get("output") == out.name
        )
        if can_skip:
            skipped += 1
            continue
        tasks.append({
            "batch_id":       batch_id,
            "out_path":       str(out),
            "files":          chunk,
            "signature":      sig,
            "conversion_signature": conv_sig,
            "file_count":     len(chunk),
            "compression":    cfg["compression"],
            "row_buffer":     DEFAULT_ROW_BUFFER,
            "row_group_size": DEFAULT_ROW_GROUP,
            "add_meta":       cfg["add_meta"],
            "placeholders":   list(PLACEHOLDERS),
            "cols_to_keep":   cfg["cols_to_keep"],
            "cols_to_drop":   cfg["cols_to_drop"],
        })

    print(f"  Batches total   : {len(batches):,}")
    if skipped:
        print(f"  Skipped (done)  : {skipped:,}")
    print(f"  Batches to run  : {len(tasks):,}")

    if not tasks:
        print("\n  Nothing left to do â€” all batches already converted!\n")
        return {"status": "skipped", "output_dir": str(output_dir), "rows": 0, "errors": 0}

    err_path  = output_dir / "conversion_errors.csv"
    error_log = ErrorLog(err_path)

    total_rows = 0
    total_errs = 0
    batch_errs = 0
    print()

    bar = tqdm(
        total=len(tasks),
        unit="batch",
        desc="  Converting",
        ascii=True,
        dynamic_ncols=True,
        bar_format=(
            "{desc}: {percentage:3.0f}%|{bar}| "
            "{n_fmt}/{total_fmt} "
            "[{elapsed}<{remaining}, {rate_fmt}]"
        ),
    )

    with ProcessPoolExecutor(max_workers=cfg["workers"]) as pool:
        futs = {pool.submit(_convert_batch, t): t for t in tasks}
        for fut in as_completed(futs):
            task        = futs[fut]
            bid         = task["batch_id"]
            try:
                res = fut.result()
            except Exception as e:
                res = {"status": "error", "rows": 0, "failed_files": [],
                       "out": task["out_path"], "error": str(e)}
            failed      = res.get("failed_files", [])
            total_rows += res.get("rows", 0)
            total_errs += len(failed)

            if res["status"] == "error":
                batch_errs += 1
                error_log.write_batch_error(bid, res)
            if failed:
                error_log.write_file_errors(bid, failed)
            if res.get("status") == "ok" and not failed:
                manifest[bid] = {
                    "signature": task["signature"],
                    "conversion_signature": task["conversion_signature"],
                    "output": Path(task["out_path"]).name,
                    "file_count": task["file_count"],
                    "columns": list(cfg["cols_to_keep"]),
                    "rows": res.get("rows", 0),
                    "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                }
                save_batch_manifest(output_dir, manifest)

            bar.set_postfix({"rows": f"{total_rows:,}", "errors": total_errs + batch_errs}, refresh=False)
            bar.update(1)

    bar.close()
    error_log.close()

    print(f"\n{'='*60}")
    print(f"  âœ“  Conversion complete!")
    print(f"  Rows written    : {total_rows:,}")
    print(f"  Batch errors    : {batch_errs:,}")
    print(f"  File errors     : {total_errs:,}")
    if total_errs or batch_errs:
        print(f"  Error log       : {err_path}")
    print(f"  Output folder   : {output_dir}")
    print(f"{'='*60}\n")

    # â”€â”€ Ask to save column preferences â”€â”€â”€â”€â”€â”€â”€â”€
    if (cfg["cols_to_keep"] or cfg["cols_to_drop"]) and not cfg.get("suppress_save_prefs", False):
        section("SAVE COLUMN PREFERENCES")
        print(f"  You kept {len(cfg['cols_to_keep'])} columns and dropped {len(cfg['cols_to_drop'])} columns.")
        print(f"  Save these so next run on a different folder asks you to reuse them?\n")
        if ask_yn("  Save column preferences?", True):
            save_prefs(cfg["cols_to_keep"], cfg["cols_to_drop"])

    print()
    status = "error" if (batch_errs or total_errs) else "ok"
    return {"status": status, "output_dir": str(output_dir), "rows": total_rows, "errors": total_errs + batch_errs}


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Multi-folder mode
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def interactive_master_menu() -> dict:
    clear()
    banner()

    section("MASTER INPUT")
    print("  Enter the master folder that contains child folders like 01, 02, 03 ...")
    print("  Each child folder will be converted into its own output folder: 01_parquet, 02_parquet, etc.\n")
    while True:
        raw_master = ask("Master folder path").strip().strip('"').strip("'")
        master_dir = Path(raw_master).expanduser()
        if master_dir.exists() and master_dir.is_dir():
            break
        print(f"  âš   Folder not found: {master_dir}\n")

    input_dirs = find_child_input_folders(master_dir)
    if not input_dirs:
        print("\n  No child folders containing .gz files were found.\n")
        sys.exit(0)

    section("FOLDERS FOUND")
    for d in input_dirs:
        out = d.parent / f"{d.name}_parquet"
        status = "output exists / smart skip will check batches" if out.exists() else "output missing / will create"
        print(f"  âœ“ {d.name}  â†’  {out.name}  ({status})")

    section("COLUMN SELECTION")
    print(f"  Scanning up to {COLUMN_SAMPLE_SIZE} .gz file(s) from each child folder to discover columns ...\n")
    all_columns = discover_columns_from_folders(input_dirs)

    if not all_columns:
        print("  âš   Could not discover any columns. Proceeding with all data.\n")
        cols_to_keep, cols_to_drop = [], []
    else:
        saved = load_prefs()
        cols_to_keep = None
        if saved:
            cols_to_keep, cols_to_drop = _reuse_saved_prefs_non_interactive(saved, all_columns)
        if cols_to_keep is None:
            cols_to_keep, cols_to_drop = column_selection_menu(all_columns)

    section("PERFORMANCE")
    print(f"  Your machine has {_CPUS} CPU cores.")
    print("  These settings will be reused for every child folder.\n")
    workers = ask_int("Worker processes", DEFAULT_WORKERS)

    print()
    print("  Batch size = how many .gz files go into one Parquet file.")
    batch_size = ask_int("Batch size (files per Parquet)", DEFAULT_BATCH_SIZE)

    section("COMPRESSION")
    print("  zstd  â†’ best size, good speed  (recommended)")
    print("  snappyâ†’ fastest, larger files")
    print("  lz4   â†’ balanced")
    print("  none  â†’ no compression (largest files, fastest write)\n")
    compression = ask_choice("Compression codec",
                             ["zstd", "snappy", "lz4", "gzip", "brotli", "none"],
                             DEFAULT_COMPRESSION)

    section("OPTIONS")
    add_meta = ask_yn("Add '_src_file' column (source filename in each row)?", False)
    # Smart resume is always ON: completed parquet batches are skipped automatically.
    resume   = True

    section("READY TO START")
    print(f"  Master folder : {master_dir}")
    print(f"  Child folders : {len(input_dirs)}")
    print(f"  Workers       : {workers}")
    print(f"  Batch size    : {batch_size} files/parquet")
    print(f"  Compression   : {compression}")
    print(f"  Add src meta  : {add_meta}")
    print("  Smart skip     : ON -- already-converted batches will be skipped automatically")
    if cols_to_keep:
        print(f"  Columns kept  : {len(cols_to_keep)}")
        print(f"  Columns drop  : {len(cols_to_drop)}")
    else:
        print("  Columns       : ALL (no filter)")
    print()

    go = ask_yn("Start converting all folders?", True)
    if not go:
        print("\n  Cancelled.\n")
        sys.exit(0)

    return {
        "mode": "master",
        "master_dir": master_dir,
        "input_dirs": input_dirs,
        "workers": workers,
        "batch_size": batch_size,
        "compression": compression,
        "add_meta": add_meta,
        "resume": resume,
        "cols_to_keep": cols_to_keep,
        "cols_to_drop": cols_to_drop,
    }


def run_master(cfg: dict):
    section("MASTER CONVERSION STARTED")
    results = []
    output_root = cfg.get("output_root")
    for idx, input_dir in enumerate(cfg["input_dirs"], 1):
        output_dir = (
            Path(output_root) / f"{input_dir.name}_parquet"
            if output_root
            else input_dir.parent / f"{input_dir.name}_parquet"
        )
        print(f"\n{'#'*60}")
        print(f"  Folder {idx}/{len(cfg['input_dirs'])}: {input_dir.name}  â†’  {output_dir.name}")
        print(f"{'#'*60}")

        child_cfg = {
            "input_dir": input_dir,
            "output_dir": output_dir,
            "workers": cfg["workers"],
            "batch_size": cfg["batch_size"],
            "compression": cfg["compression"],
            "add_meta": cfg["add_meta"],
            "resume": cfg["resume"],
            "cols_to_keep": cfg["cols_to_keep"],
            "cols_to_drop": cfg["cols_to_drop"],
            "suppress_save_prefs": True,
        }
        results.append((input_dir.name, run(child_cfg)))

    print(f"\n{'='*60}")
    print("  âœ“  Master conversion complete!")
    for name, res in results:
        print(f"  {name:<20} â†’ {res.get('status', 'unknown'):<8} rows={res.get('rows', 0):,} errors={res.get('errors', 0):,}")
    print(f"{'='*60}\n")

    failed = [(name, res) for name, res in results if res.get("status") not in {"ok", "skipped"}]
    if failed:
        names = ", ".join(name for name, _ in failed)
        raise SystemExit(f"001.py failed for {len(failed)} folder(s): {names}")

    if cfg["cols_to_keep"] or cfg["cols_to_drop"]:
        section("SAVE COLUMN PREFERENCES")
        print(f"  You kept {len(cfg['cols_to_keep'])} columns and dropped {len(cfg['cols_to_drop'])} columns.")
        if ask_yn("  Save column preferences?", True):
            save_prefs(cfg["cols_to_keep"], cfg["cols_to_drop"])


def _env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        return int(raw.strip())
    except ValueError:
        return default


def _build_001_config_from_env() -> dict | None:
    mode = (os.getenv("VG_ETL_001_MODE", "").strip().lower())
    if not mode:
        return None

    workers = _env_int("VG_ETL_001_WORKERS", DEFAULT_WORKERS)
    batch_size = _env_int("VG_ETL_001_BATCH_SIZE", DEFAULT_BATCH_SIZE)
    compression = os.getenv("VG_ETL_001_COMPRESSION", DEFAULT_COMPRESSION)
    add_meta = _env_bool("VG_ETL_001_ADD_META", False)
    resume = _env_bool("VG_ETL_001_RESUME", True)

    keep_raw = os.getenv("VG_ETL_001_KEEP_COLUMNS")
    drop_raw = os.getenv("VG_ETL_001_DROP_COLUMNS")
    cols_to_keep = [c.strip() for c in keep_raw.split(",") if c.strip()] if keep_raw else []
    cols_to_drop = [c.strip() for c in drop_raw.split(",") if c.strip()] if drop_raw else []

    if mode in {"2", "master"}:
        master_dir_raw = os.getenv("VG_ETL_001_MASTER_DIR")
        if not master_dir_raw:
            raise SystemExit("VG_ETL_001_MODE=master requires VG_ETL_001_MASTER_DIR")
        master_dir = Path(master_dir_raw).expanduser().resolve()
        if not master_dir.exists() or not master_dir.is_dir():
            raise SystemExit(f"Master folder not found: {master_dir}")
        output_root_raw = os.getenv("VG_ETL_001_OUTPUT_ROOT")
        output_root = Path(output_root_raw).expanduser().resolve() if output_root_raw else None

        input_dirs = find_child_input_folders(master_dir)
        if not input_dirs:
            raise SystemExit(f"No child input folders found in: {master_dir}")

        if not cols_to_keep and not cols_to_drop:
            all_columns = discover_columns_from_folders(input_dirs)
            if all_columns:
                saved = load_prefs()
                if saved:
                    cols_to_keep, cols_to_drop = _reuse_saved_prefs_non_interactive(saved, all_columns)

        return {
            "mode": "master",
            "master_dir": master_dir,
            "input_dirs": input_dirs,
            "output_root": output_root,
            "workers": workers,
            "batch_size": batch_size,
            "compression": compression,
            "add_meta": add_meta,
            "resume": resume,
            "cols_to_keep": cols_to_keep,
            "cols_to_drop": cols_to_drop,
            "suppress_save_prefs": True,
        }

    if mode in {"1", "single"}:
        input_raw = os.getenv("VG_ETL_001_INPUT_DIR")
        if not input_raw:
            raise SystemExit("VG_ETL_001_MODE=single requires VG_ETL_001_INPUT_DIR")
        input_dir = Path(input_raw).expanduser().resolve()
        if not input_dir.exists() or not input_dir.is_dir():
            raise SystemExit(f"Input folder not found: {input_dir}")

        output_dir = Path(
            os.getenv("VG_ETL_001_OUTPUT_DIR", str(input_dir.parent / f"{input_dir.name}_parquet"))
        ).expanduser().resolve()

        if not cols_to_keep and not cols_to_drop:
            all_columns = discover_columns(input_dir)
            if all_columns:
                saved = load_prefs()
                if saved:
                    cols_to_keep, cols_to_drop = _reuse_saved_prefs_non_interactive(saved, all_columns)

        return {
            "mode": "single",
            "input_dir": input_dir,
            "output_dir": output_dir,
            "workers": workers,
            "batch_size": batch_size,
            "compression": compression,
            "add_meta": add_meta,
            "resume": resume,
            "cols_to_keep": cols_to_keep,
            "cols_to_drop": cols_to_drop,
            "suppress_save_prefs": True,
        }

    raise SystemExit(f"Unsupported VG_ETL_001_MODE={mode}. Use master or single.")


def choose_mode() -> str:
    clear()
    banner()
    section("MODE")
    print("  1) Single folder mode  â†’ convert one folder")
    print("  2) Master folder mode  â†’ convert child folders like 01, 02, 03 automatically\n")
    return ask_choice("Choose mode", ["1", "2"], "2")


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Entry point
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

if __name__ == "__main__":
    cfg = _build_001_config_from_env()
    if cfg is not None:
        if cfg["mode"] == "master":
            print("Running 001.py in non-interactive mode: MASTER")
            run_master(cfg)
        else:
            print("Running 001.py in non-interactive mode: SINGLE")
            result = run(cfg)
            if result.get("status") not in {"ok", "skipped"}:
                raise SystemExit("001.py failed in single-folder mode.")
    else:
        mode = choose_mode()
        if mode == "2":
            cfg = interactive_master_menu()
            print()
            run_master(cfg)
        else:
            cfg = interactive_menu()
            print()
            run(cfg)




