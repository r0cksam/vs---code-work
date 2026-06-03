import os
import sys
import csv
import gzip
import time
import platform
from datetime import datetime
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
DEFAULT_BATCH_SIZE  = 2000
DEFAULT_COMPRESSION = "zstd"
DEFAULT_ROW_BUFFER  = 100_000
DEFAULT_ROW_GROUP   = 200_000
PLACEHOLDERS        = {"-", "^"}


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
    """Ask a question, return stripped answer (or default if blank)."""
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
    print()

    go = ask_yn("Start conversion?", True)
    if not go:
        print("\n  Cancelled.\n")
        sys.exit(0)

    return {
        "input_dir":   input_dir,
        "output_dir":  output_dir,
        "workers":     workers,
        "batch_size":  batch_size,
        "compression": compression,
        "add_meta":    add_meta,
        "resume":      resume,
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
            schema = pa.schema([pa.field(c, pa.string()) for c in buf[0].keys()])
        tbl = pa.Table.from_pylist(buf, schema=schema)
        if writer is None:
            writer = pq.ParquetWriter(str(tmp_path), schema, compression=compress)
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
            writer and writer.close()
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
        self._csv.writerow({"timestamp": datetime.utcnow().isoformat(),
                            "batch": bid, "stage": "batch",
                            "file": result.get("out", ""),
                            "error": result.get("error", "")})
        self._count += 1
        self._fh.flush()

    def write_file_errors(self, bid, items):
        ts = datetime.utcnow().isoformat()
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
    paths = [str(p) for p in input_dir.rglob("*.gz")]
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
            task         = futs[fut]
            res          = fut.result()
            bid          = task["batch_id"]
            failed       = res.get("failed_files", [])
            total_rows  += res.get("rows", 0)
            total_errs  += len(failed)

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
        print(f"  (open the CSV to see which files failed and why)")
    print(f"  Output folder   : {output_dir}")
    print(f"{'='*60}\n")


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────

if __name__ == "__main__":
    cfg = interactive_menu()
    print()
    run(cfg)