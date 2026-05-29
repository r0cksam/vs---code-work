#!/usr/bin/env python3
"""
verify_lake_report_env.py
═══════════════════════════════════════════════════════════════════════
Pre-flight check for lake_report_generator.

Verifies:
  1. Python version (≥ 3.8)
  2. Required packages are installed
  3. Lake folder exists and has correct Hive partition structure
  4. At least one Parquet file exists
  5. Output folder is writable
  6. Enough free disk space for DuckDB temp spill (optional warning)
  7. Network drive accessibility (if UNC path)
"""

import sys
import os
import importlib
from pathlib import Path
from datetime import datetime

# ──────────────────────────────────────────────────────────────────────
# 1. Python version
# ──────────────────────────────────────────────────────────────────────
required_python = (3, 8)
if sys.version_info < required_python:
    print(f"❌ Python {required_python[0]}.{required_python[1]}+ required (found {sys.version_info.major}.{sys.version_info.minor})")
    sys.exit(1)
else:
    print(f"✅ Python {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")

# ──────────────────────────────────────────────────────────────────────
# 2. Required packages
# ──────────────────────────────────────────────────────────────────────
required_packages = [
    "duckdb",
    "pandas",
    "pyarrow",
    "xlsxwriter",
    "psutil",
]
missing_packages = []
for pkg in required_packages:
    try:
        importlib.import_module(pkg)
        print(f"✅ {pkg}")
    except ImportError:
        missing_packages.append(pkg)
        print(f"❌ {pkg} – missing")

if missing_packages:
    print(f"\n❌ Install missing packages with:\n    pip install {' '.join(missing_packages)}")
    sys.exit(1)

# ──────────────────────────────────────────────────────────────────────
# 3. Lake folder validation
# ──────────────────────────────────────────────────────────────────────
lake_input = input("\n📁 Enter lake folder path to verify: ").strip()
lake_path = Path(lake_input).resolve()

print(f"\nChecking: {lake_path}")

if not lake_path.exists():
    print(f"❌ Folder does not exist: {lake_path}")
    sys.exit(1)
else:
    print(f"✅ Folder exists")

# Check for year partitions
year_dirs = [d for d in lake_path.iterdir() if d.is_dir() and d.name.startswith("year=")]
if not year_dirs:
    print(f"⚠️  No 'year=*' partitions found. This may still be valid if you use absolute file paths.")
else:
    print(f"✅ Found {len(year_dirs)} year partitions: {', '.join(d.name for d in year_dirs[:5])}{' ...' if len(year_dirs)>5 else ''}")

# Check for month partitions (inside first year)
first_year = None
for yd in year_dirs[:1]:
    month_dirs = [d for d in yd.iterdir() if d.is_dir() and d.name.startswith("month=")]
    if month_dirs:
        print(f"✅ Found {len(month_dirs)} month partitions inside {yd.name}")
        first_year = yd
        break

# Check for day partitions (inside first month of first year)
if first_year:
    first_month = None
    for md in first_year.iterdir():
        if md.is_dir() and md.name.startswith("month="):
            day_dirs = [d for d in md.iterdir() if d.is_dir() and d.name.startswith("day=")]
            if day_dirs:
                print(f"✅ Found {len(day_dirs)} day partitions inside {md.name}")
                first_month = md
                break
    if not first_month:
        print("⚠️  No day partitions found inside month directories. Structure may be incomplete.")

# Count Parquet files
parquet_files = list(lake_path.rglob("*.parquet"))
if not parquet_files:
    print("❌ No .parquet files found anywhere under the lake folder.")
    sys.exit(1)
else:
    print(f"✅ Found {len(parquet_files)} Parquet files")

# Sample one Parquet file to see if schema is readable
sample = parquet_files[0]
try:
    import pyarrow.parquet as pq
    schema = pq.read_schema(sample)
    col_names = [name for name in schema.names if name not in ('year','month','day')]
    print(f"✅ Sample Parquet schema: {len(col_names)} data columns (e.g. {', '.join(col_names[:5])}{' ...' if len(col_names)>5 else ''})")
except Exception as e:
    print(f"⚠️  Could not read Parquet schema from {sample.name}: {e}")

# ──────────────────────────────────────────────────────────────────────
# 4. Output folder writability
# ──────────────────────────────────────────────────────────────────────
output_default = lake_path.parent / "lake_report.xlsx"
output_choice = input(f"\n💾 Output folder check (Enter = {output_default.parent}): ").strip()
output_path = Path(output_choice).resolve() if output_choice else output_default
try:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    test_file = output_path.parent / ".write_test"
    test_file.touch()
    test_file.unlink()
    print(f"✅ Writable: {output_path.parent}")
except Exception as e:
    print(f"❌ Cannot write to {output_path.parent}: {e}")
    sys.exit(1)

# ──────────────────────────────────────────────────────────────────────
# 5. Disk space (warning only)
# ──────────────────────────────────────────────────────────────────────
try:
    import shutil
    usage = shutil.disk_usage(output_path.parent)
    free_gb = usage.free / (1024**3)
    if free_gb < 10:
        print(f"⚠️  Low free disk space on {output_path.parent}: {free_gb:.1f} GiB free (DuckDB may need space for temporary files)")
    else:
        print(f"✅ Disk space: {free_gb:.1f} GiB free")
except Exception as e:
    print(f"⚠️  Could not check disk space: {e}")

# ──────────────────────────────────────────────────────────────────────
# 6. Network drive accessibility (for UNC paths)
# ──────────────────────────────────────────────────────────────────────
if str(lake_path).startswith("\\\\"):
    print("ℹ️  Lake folder is on a network UNC path. Ensure stable connection.")
    # Simple test: open a small Parquet file
    try:
        import pyarrow.parquet as pq
        meta = pq.read_metadata(sample)
        print(f"✅ Read metadata from {sample.name} (rows: {meta.num_rows}) – network OK")
    except Exception as e:
        print(f"⚠️  Could not read Parquet metadata across network: {e}")

# ──────────────────────────────────────────────────────────────────────
# 7. Required columns (based on column names in first file)
# ──────────────────────────────────────────────────────────────────────
expected_columns = [
    "UA", "asn", "city", "state", "country", "reqHost",
    "queryStr", "cliIP", "reqTimeSec"
]
try:
    import pyarrow.parquet as pq
    schema = pq.read_schema(sample)
    present = set(schema.names)
    missing = [c for c in expected_columns if c not in present]
    if missing:
        print(f"⚠️  Missing expected columns: {', '.join(missing)} (may be renamed in your Parquet)")
    else:
        print(f"✅ All expected columns present: {', '.join(expected_columns)}")
except:
    pass

# ──────────────────────────────────────────────────────────────────────
# Summary
# ──────────────────────────────────────────────────────────────────────
print("\n" + "═" * 58)
print("  Verification complete. The environment is ready.")
print("  You can now run the main report generator.")
print("═" * 58)

# Optional: run a quick integrity check with DuckDB?
run_db_check = input("\nRun a fast DuckDB connection test? (y/n): ").strip().lower()
if run_db_check == 'y':
    import duckdb
    try:
        con = duckdb.connect()
        # Just try to read one row from the first Parquet file
        result = con.execute(f"SELECT COUNT(*) FROM read_parquet('{sample.as_posix()}')").fetchone()
        print(f"✅ DuckDB can read {sample.name}: {result[0]} rows")
        con.close()
    except Exception as e:
        print(f"❌ DuckDB test failed: {e}")
        sys.exit(1)

print("\n✅ All systems go. Proceed to main script.")