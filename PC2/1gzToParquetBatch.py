import os
import sys
import csv
import gzip
import json
import time
import platform
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

import orjson
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm


# ─────────────────────────────────────────────
# Defaults
# ─────────────────────────────────────────────
_CPUS               = os.cpu_count() or 4
DEFAULT_WORKERS     = min(max(1, _CPUS - 1), 32)
DEFAULT_BATCH_SIZE  = 10000
DEFAULT_COMPRESSION = "zstd"
DEFAULT_ROW_BUFFER  = 100_000
DEFAULT_ROW_GROUP   = 200_000
PLACEHOLDERS        = {"-", "^"}

PREFS_FILE          = Path.home() / ".gz_parquet_prefs.json"
COLUMN_SAMPLE_SIZE  = 1          # how many .gz files to scan for column discovery


# ─────────────────────────────────────────────
# Pretty helpers
# ─────────────────────────────────────────────

def clear():
    os.system("cls" if platform.system() == "Windows" else "clear")

def banner():
    print("=" * 60)
    print("   GZ  →  PARQUET  Converter")
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
            print(f"  ⚠  Please enter a whole number.\n")

def ask_choice(prompt: str, choices: list[str], default: str) -> str:
    choices_str = " / ".join(
        f"[{c}]" if c == default else c for c in choices
    )
    while True:
        raw = ask(f"{prompt}  ({choices_str})", default)
        if raw in choices:
            return raw
        print(f"  ⚠  Choose one of: {', '.join(choices)}\n")

def ask_yn(prompt: str, default: bool) -> bool:
    hint = "Y/n" if default else "y/N"
    raw  = ask(f"{prompt}  ({hint})", "y" if default else "n").lower()
    return raw in ("y", "yes")

def section(title: str):
    print()
    print(f"  ── {title} " + "─" * max(0, 48 - len(title)))


# ─────────────────────────────────────────────
# Saved column preferences
# ─────────────────────────────────────────────

def load_prefs() -> dict | None:
    """Load saved column prefs from home dir. Returns None if not found."""
    try:
        if PREFS_FILE.exists():
            with PREFS_FILE.open("r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return None


def save_prefs(cols_to_keep: list[str], cols_to_drop: list[str]):
    """Save column selection to home dir for future runs."""
    data = {
        "saved_at":    datetime.now(timezone.utc).isoformat(),
        "cols_to_keep": cols_to_keep,
        "cols_to_drop": cols_to_drop,
    }
    try:
        with PREFS_FILE.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        print(f"\n  ✓  Preferences saved → {PREFS_FILE}")
    except Exception as e:
        print(f"\n  ⚠  Could not save prefs: {e}")


# ─────────────────────────────────────────────
# Column discovery
# ─────────────────────────────────────────────

def discover_columns(input_dir: Path, sample_size: int = COLUMN_SAMPLE_SIZE) -> list[str]:
    """
    Scan up to `sample_size` .gz files and collect all unique top-level keys.
    Returns a sorted list of column names.
    """
    all_paths = list(input_dir.rglob("*.gz"))
    sample    = all_paths[:sample_size]          # first N — fast enough for discovery

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
                    if len(seen) > 5000:          # safety cap — stop scanning early
                        break
        except Exception:
            continue

    return sorted(seen)


# ─────────────────────────────────────────────
# Column selection menu
# ─────────────────────────────────────────────

def _print_columns(columns: list[str], dropped: set[str]):
    """Pretty-print numbered column list with keep/drop status."""
    print()
    print(f"  {'#':<5}  {'STATUS':<8}  COLUMN")
    print(f"  {'─'*5}  {'─'*8}  {'─'*40}")
    for i, col in enumerate(columns, 1):
        status = "DROP  ✗" if col in dropped else "keep  ✓"
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
        print("  ⚠  No columns discovered. All data will be kept as-is.")
        return []

    dropped: set[str] = set()

    print()
    print(f"  Found {len(columns)} unique columns across sampled files.")
    _print_columns(columns, dropped)

    print()
    print("  How to select:")
    print("    • Type column numbers to DROP  (e.g.  3 7 12)")
    print("    • Type  'reset'  to restore all columns")
    print("    • Type  'done'   (or press Enter) when finished")
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
            print("  ⚠  Enter column numbers separated by spaces or commas.\n")
            continue

        for n in nums:
            col = columns[n - 1]
            if col in dropped:
                dropped.discard(col)          # toggle back on
                print(f"  ✓  Restored  → {col}")
            else:
                dropped.add(col)
                print(f"  ✗  Dropping  → {col}")

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

    # Intersect with columns actually present in this dataset
    valid_keep = [c for c in saved_keep if c in discovered]
    valid_drop = [c for c in saved_drop if c in discovered]
    new_cols   = [c for c in discovered if c not in saved_keep and c not in saved_drop]

    print(f"\n  ┌─  Saved column preferences found  (saved: {saved_at[:19]})")
    print(f"  │   Keeping  : {len(valid_keep)} columns")
    print(f"  │   Dropping : {len(valid_drop)} columns")
    if new_cols:
        print(f"  │   NEW cols not in saved prefs : {len(new_cols)}")
        for c in new_cols[:10]:
            print(f"  │     + {c}")
        if len(new_cols) > 10:
            print(f"  │     ... and {len(new_cols)-10} more")
    print(f"  └{'─'*50}")

    use_saved = ask_yn("  Use saved preferences?", True)
    if use_saved:
        # New columns default to keep — user can override in full menu if they want
        all_keep = valid_keep + new_cols
        return all_keep, valid_drop

    return None          # signal: go to full selection menu


# ─────────────────────────────────────────────
# Interactive menu
# ─────────────────────────────────────────────

def interactive_menu() -> dict:
    clear()
    banner()

    # ── Input folder ──────────────────────────
    section("INPUT")
    print("  Enter the folder that contains your .gz files.")
    print("  Subfolders are included automatically.\n")
    while True:
        input_dir = Path(ask("Input folder path")).expanduser().resolve()
        if input_dir.exists() and input_dir.is_dir():
            break
        print(f"  ⚠  Folder not found: {input_dir}\n")

    # ── Output folder ─────────────────────────
    section("OUTPUT")
    default_out = str(input_dir.parent / (input_dir.name + "_parquet"))
    print(f"  Where should Parquet files be saved?\n")
    output_dir = Path(ask("Output folder path", default_out)).expanduser().resolve()

    # ── Column discovery & selection ──────────
    section("COLUMN SELECTION")
    print(f"  Scanning up to {COLUMN_SAMPLE_SIZE} files to discover columns ...\n")
    all_columns = discover_columns(input_dir)

    if not all_columns:
        print("  ⚠  Could not discover any columns. Proceeding with all data.\n")
        cols_to_keep, cols_to_drop = [], []
    else:
        saved = load_prefs()
        cols_to_keep = None

        if saved:
            result = _offer_saved_prefs(saved, all_columns)
            if result is not None:
                cols_to_keep, cols_to_drop = result

        if cols_to_keep is None:
            # Full interactive selection
            cols_to_keep, cols_to_drop = column_selection_menu(all_columns)

    # ── Workers ───────────────────────────────
    section("PERFORMANCE")
    print(f"  Your machine has {_CPUS} CPU cores.")
    print(f"  More workers = faster, but uses more RAM.\n")
    workers = ask_int(f"Worker processes", DEFAULT_WORKERS)

    # ── Batch size ────────────────────────────
    print()
    print("  Batch size = how many .gz files go into one Parquet file.")
    print("  Larger = fewer output files, slightly more RAM per worker.\n")
    batch_size = ask_int("Batch size (files per Parquet)", DEFAULT_BATCH_SIZE)

    # ── Compression ───────────────────────────
    section("COMPRESSION")
    print("  zstd  → best size, good speed  (recommended)")
    print("  snappy→ fastest, larger files")
    print("  lz4   → balanced")
    print("  none  → no compression (largest files, fastest write)\n")
    compression = ask_choice("Compression codec",
                             ["zstd", "snappy", "lz4", "gzip", "brotli", "none"],
                             "zstd")

    # ── Options ───────────────────────────────
    section("OPTIONS")
    add_meta = ask_yn("Add '_src_file' column (source filename in each row)?", False)
    resume   = ask_yn("Skip already-converted batches if restarting?", True)

    # ── Confirm ───────────────────────────────
    section("READY TO START")
    print(f"  Input folder  : {input_dir}")
    print(f"  Output folder : {output_dir}")
    print(f"  Workers       : {workers}")
    print(f"  Batch size    : {batch_size} files/parquet")
    print(f"  Compression   : {compression}")
    print(f"  Add src meta  : {add_meta}")
    print(f"  Resume mode   : {resume}")
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


# ─────────────────────────────────────────────
# Worker (subprocess) — no I/O to terminal
# ─────────────────────────────────────────────

def _convert_batch(task: dict) -> dict:
    out_path  = Path(task["out_path"])
    tmp_path  = out_path.with_suffix(".parquet.tmp")
    files     = task["files"]
    compress  = task["compression"]
    buf_size  = task["row_buffer"]
    rg_size   = task["row_group_size"]
    add_meta  = task["add_meta"]
    null_set  = set(task["placeholders"])
    keep_set  = set(task["cols_to_keep"])   # empty = keep all
    filter_cols = bool(keep_set)

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
            all_cols = sorted({k for row in buf for k in row.keys()})
            schema = pa.schema([pa.field(c, pa.string()) for c in all_cols])

        cols = schema.names
        normalized = [{c: row.get(c) for c in cols} for row in buf]

        tbl = pa.Table.from_pylist(normalized, schema=schema)

        if writer is None:
            writer = pq.ParquetWriter(
                str(tmp_path),
                schema,
                compression=compress,
                compression_level=3 if compress == "zstd" else None
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
                            # ── column filter ──
                            if filter_cols and k not in keep_set:
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


# ─────────────────────────────────────────────
# Error log
# ─────────────────────────────────────────────

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


# ─────────────────────────────────────────────
# Pipeline
# ─────────────────────────────────────────────

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
        sys.exit(0)

    bs      = cfg["batch_size"]
    batches = [paths[i:i+bs] for i in range(0, len(paths), bs)]

    tasks   = []
    skipped = 0
    for idx, chunk in enumerate(batches):
        out = output_dir / f"part_{idx:07d}.parquet"
        if cfg["resume"] and out.exists() and out.stat().st_size > 0:
            skipped += 1
            continue
        tasks.append({
            "batch_id":       f"batch_{idx:07d}",
            "out_path":       str(out),
            "files":          chunk,
            "compression":    cfg["compression"],
            "row_buffer":     DEFAULT_ROW_BUFFER,
            "row_group_size": DEFAULT_ROW_GROUP,
            "add_meta":       cfg["add_meta"],
            "placeholders":   list(PLACEHOLDERS),
            "cols_to_keep":   cfg["cols_to_keep"],
        })

    print(f"  Batches total   : {len(batches):,}")
    if skipped:
        print(f"  Skipped (done)  : {skipped:,}")
    print(f"  Batches to run  : {len(tasks):,}")

    if not tasks:
        print("\n  Nothing left to do — all batches already converted!\n")
        sys.exit(0)

    err_path  = output_dir / "conversion_errors.csv"
    error_log = ErrorLog(err_path)

    total_rows = 0
    total_errs = 0
    print()

    bar = tqdm(
        total=len(tasks),
        unit="batch",
        desc="  Converting",
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
                error_log.write_batch_error(bid, res)
            if failed:
                error_log.write_file_errors(bid, failed)

            bar.set_postfix({"rows": f"{total_rows:,}", "errors": total_errs}, refresh=False)
            bar.update(1)

    bar.close()
    error_log.close()

    print(f"\n{'='*60}")
    print(f"  ✓  Conversion complete!")
    print(f"  Rows written    : {total_rows:,}")
    print(f"  File errors     : {total_errs:,}")
    if total_errs:
        print(f"  Error log       : {err_path}")
    print(f"  Output folder   : {output_dir}")
    print(f"{'='*60}\n")

    # ── Ask to save column preferences ────────
    if cfg["cols_to_keep"] or cfg["cols_to_drop"]:
        section("SAVE COLUMN PREFERENCES")
        print(f"  You kept {len(cfg['cols_to_keep'])} columns and dropped {len(cfg['cols_to_drop'])} columns.")
        print(f"  Save these so next run on a different folder asks you to reuse them?\n")
        if ask_yn("  Save column preferences?", True):
            save_prefs(cfg["cols_to_keep"], cfg["cols_to_drop"])
    print()


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────

if __name__ == "__main__":
    cfg = interactive_menu()
    print()
    run(cfg)