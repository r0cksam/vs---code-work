"""
inspect_parquet_schema.py
Quickly inspect parquet folders without loading full data into RAM.

Run:
  python inspect_parquet_schema.py --input "D:\\Veto Logs\\05 Veto Logs Backup\\02_final_clean.parquet" --sample-rows 5
"""

import argparse
from pathlib import Path
from collections import Counter, defaultdict

import duckdb
import pyarrow.parquet as pq


def qident(x: str) -> str:
    return '"' + str(x).replace('"', '""') + '"'


def sql_str(x: str) -> str:
    return "'" + str(x).replace("'", "''") + "'"


def sql_list(values):
    return "[" + ", ".join(sql_str(v) for v in values) + "]"


def collect_files(paths, recursive=False):
    out = []
    for raw in paths:
        p = Path(raw)
        if p.is_file() and p.suffix.lower() == ".parquet":
            out.append(str(p.resolve()))
        elif p.is_dir():
            pat = "**/*.parquet" if recursive else "*.parquet"
            out.extend(str(x.resolve()) for x in sorted(p.glob(pat)) if x.is_file())
    return sorted(dict.fromkeys(out))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", nargs="+", required=True, help="Parquet file/folder path(s)")
    ap.add_argument("--recursive", action="store_true")
    ap.add_argument("--sample-rows", type=int, default=5)
    ap.add_argument("--check-cols", nargs="*", default=[
        "queryStr", "reqTimeSec", "reqPath", "UA", "country", "region", "city",
        "device_id", "session_id", "channel", "channel_name", "platform", "device"
    ])
    args = ap.parse_args()

    files = collect_files(args.input, args.recursive)
    if not files:
        raise SystemExit("No parquet files found.")

    print(f"Files: {len(files):,}")
    print(f"First file: {files[0]}")

    total_rows = 0
    total_size = 0
    col_types = {}
    col_files = Counter()
    file_rows = []

    for f in files:
        p = Path(f)
        total_size += p.stat().st_size
        try:
            meta = pq.read_metadata(f)
            rows = int(meta.num_rows or 0)
            total_rows += rows
            schema = pq.read_schema(f)
            file_rows.append((p.name, rows, p.stat().st_size / (1024**3)))
            for field in schema:
                col_types.setdefault(field.name, str(field.type))
                col_files[field.name] += 1
        except Exception as e:
            print(f"WARNING reading {f}: {e}")

    print(f"Total rows from metadata: {total_rows:,}")
    print(f"Total size: {total_size / (1024**3):,.2f} GB")
    print(f"Columns union: {len(col_types):,}")

    print("\nTop files by rows:")
    for name, rows, size_gb in sorted(file_rows, key=lambda x: x[1], reverse=True)[:10]:
        print(f"  {name:45s} rows={rows:15,} size={size_gb:8.2f} GB")

    print("\nColumn inventory:")
    for name in sorted(col_types):
        missing = len(files) - col_files[name]
        miss_txt = "" if missing == 0 else f"  MISSING_IN_FILES={missing}"
        print(f"  {name:35s} {col_types[name]:18s} files={col_files[name]:3d}/{len(files)}{miss_txt}")

    print("\nImportant column availability:")
    for c in args.check_cols:
        if c in col_types:
            print(f"  OK      {c:25s} {col_types[c]}")
        else:
            similar = [x for x in col_types if c.lower() in x.lower() or x.lower() in c.lower()]
            hint = f" | maybe: {similar[:8]}" if similar else ""
            print(f"  MISSING {c:25s}{hint}")

    con = duckdb.connect()
    files_sql = sql_list(files)

    print("\nSample rows for available important columns:")
    available = [c for c in args.check_cols if c in col_types]
    if available:
        select_cols = ", ".join(qident(c) for c in available)
        try:
            df = con.execute(f"SELECT {select_cols} FROM read_parquet({files_sql}, union_by_name=true) LIMIT {int(args.sample_rows)}").df()
            print(df.to_string(index=False, max_colwidth=80))
        except Exception as e:
            print(f"Could not sample rows: {e}")

    print("\nDistinct sample values for location columns:")
    for c in ["country", "region", "city"]:
        if c in col_types:
            try:
                df = con.execute(f"""
                    SELECT CAST({qident(c)} AS VARCHAR) AS value, COUNT(*) AS rows
                    FROM read_parquet({files_sql}, union_by_name=true)
                    GROUP BY 1
                    ORDER BY rows DESC
                    LIMIT 20
                """).df()
                print(f"\n{c}:")
                print(df.to_string(index=False, max_colwidth=80))
            except Exception as e:
                print(f"Could not count {c}: {e}")


if __name__ == "__main__":
    main()
