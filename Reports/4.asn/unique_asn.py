#!/usr/bin/env python
"""
asn_summary_with_names.py
=========================

Create an ASN summary with ASN organization names.

Works in two modes:

1) From parquet folder/glob:
   python asn_summary_with_names.py ^
     --input "D:\Vs - Code Work\cleaned_output" ^
     --asn-col asn ^
     --output "D:\Vs - Code Work\asn_summary_with_names.csv"

2) From an existing CSV with columns like value,count,% of rows:
   python asn_summary_with_names.py ^
     --csv "D:\Veto Logs Backup\Reports\unique_asn.csv" ^
     --output "D:\Veto Logs Backup\Reports\asn_summary_with_names.csv"

It uses Team Cymru's public ASN whois service to map ASN number -> ASN name.
Results are cached locally in asn_lookup_cache.json so repeated runs are faster.

If internet/whois is blocked, the script still writes the summary with asn_name="Unknown".
"""

from __future__ import annotations

import argparse
import json
import socket
import sys
from pathlib import Path
from typing import Dict, List

import pandas as pd


def parse_args():
    p = argparse.ArgumentParser(description="Build ASN summary with ASN names.")
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--input", help="Parquet folder or parquet glob, e.g. D:\\data or D:\\data\\*.parquet")
    src.add_argument("--csv", help="Existing CSV with ASN counts, e.g. columns value,count,% of rows")

    p.add_argument("--asn-col", default="asn", help="ASN column name in parquet. Default: asn")
    p.add_argument("--output", default="asn_summary_with_names.csv", help="Output CSV path")
    p.add_argument("--cache", default="asn_lookup_cache.json", help="ASN lookup cache JSON path")
    p.add_argument("--top", type=int, default=0, help="Only keep top N ASNs. 0 = all.")
    p.add_argument("--threads", type=int, default=6, help="DuckDB threads when reading parquet")
    p.add_argument("--no-lookup", action="store_true", help="Do not call Team Cymru; output only ASN/count/pct.")
    p.add_argument("--timeout", type=int, default=20, help="Whois timeout seconds")
    return p.parse_args()


def normalize_asn(x) -> str:
    """Normalize ASN values like 24560, AS24560, '24560.0' -> '24560'."""
    if pd.isna(x):
        return ""
    s = str(x).strip()
    if not s or s.lower() in {"nan", "none", "null", "(null)", "-", "^"}:
        return ""
    s = s.upper().replace("AS", "").strip()
    # Handle values that arrived as float strings
    if s.endswith(".0"):
        s = s[:-2]
    # Keep only digits
    s = "".join(ch for ch in s if ch.isdigit())
    return s


def parquet_files(input_path: str) -> List[str]:
    p = Path(input_path)
    if any(ch in input_path for ch in ["*", "?", "["]):
        files = sorted(Path().glob(input_path)) if not Path(input_path).is_absolute() else sorted(Path(p.anchor).glob(str(p)[len(p.anchor):]))
        # Glob handling above is awkward on Windows with drive letters; fallback to glob module.
        if not files:
            import glob
            files = [Path(x) for x in glob.glob(input_path)]
    elif p.is_dir():
        files = sorted(p.glob("*.parquet"))
    elif p.is_file() and p.suffix.lower() == ".parquet":
        files = [p]
    else:
        import glob
        files = [Path(x) for x in glob.glob(input_path)]

    files = [str(f) for f in files if str(f).lower().endswith(".parquet")]
    if not files:
        raise FileNotFoundError(f"No parquet files found for: {input_path}")
    return files


def build_summary_from_parquet(input_path: str, asn_col: str, threads: int) -> pd.DataFrame:
    try:
        import duckdb
    except ImportError:
        raise RuntimeError("duckdb is required for parquet mode. Install with: pip install duckdb")

    files = parquet_files(input_path)
    con = duckdb.connect()
    con.execute(f"PRAGMA threads={int(threads)}")

    qcol = '"' + asn_col.replace('"', '""') + '"'

    df = con.execute(f"""
        WITH base AS (
            SELECT
                NULLIF(TRIM(CAST({qcol} AS VARCHAR)), '') AS asn_raw
            FROM read_parquet(?, union_by_name=true)
        ),
        grouped AS (
            SELECT
                asn_raw,
                COUNT(*) AS requests
            FROM base
            WHERE asn_raw IS NOT NULL
              AND asn_raw NOT IN ('-', '^', 'nan', 'None', 'NULL', '(null)')
            GROUP BY 1
        )
        SELECT
            asn_raw AS asn,
            requests,
            ROUND(requests * 100.0 / SUM(requests) OVER (), 4) AS pct_rows
        FROM grouped
        ORDER BY requests DESC
    """, [files]).df()

    return df


def build_summary_from_csv(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)

    # Accept common shapes:
    # value,count,% of rows
    # asn,count,pct_rows
    # ASN,requests,pct_rows
    lower = {c.lower().strip(): c for c in df.columns}

    asn_col = None
    for cand in ["asn", "value", "as_number", "as"]:
        if cand in lower:
            asn_col = lower[cand]
            break
    if asn_col is None:
        asn_col = df.columns[0]

    count_col = None
    for cand in ["requests", "count", "rows"]:
        if cand in lower:
            count_col = lower[cand]
            break
    if count_col is None:
        count_col = df.columns[1]

    pct_col = None
    for cand in ["pct_rows", "% of rows", "percent", "percentage"]:
        if cand in lower:
            pct_col = lower[cand]
            break

    out = pd.DataFrame({
        "asn": df[asn_col],
        "requests": pd.to_numeric(df[count_col], errors="coerce").fillna(0).astype("int64"),
    })
    if pct_col:
        out["pct_rows"] = pd.to_numeric(df[pct_col], errors="coerce").fillna(0.0)
    else:
        total = out["requests"].sum()
        out["pct_rows"] = (out["requests"] / total * 100).round(4) if total else 0.0

    return out.sort_values("requests", ascending=False).reset_index(drop=True)


def load_cache(cache_path: Path) -> Dict[str, dict]:
    if not cache_path.exists():
        return {}
    try:
        with cache_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return {str(k): dict(v) for k, v in data.items()}
    except Exception:
        return {}


def save_cache(cache_path: Path, cache: Dict[str, dict]) -> None:
    try:
        with cache_path.open("w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2, sort_keys=True)
    except Exception as e:
        print(f"WARNING: Could not write cache {cache_path}: {e}")


def team_cymru_bulk_lookup(asns: List[str], timeout: int = 20) -> Dict[str, dict]:
    """
    Bulk ASN lookup using Team Cymru whois service.

    Response format with verbose mode:
    AS | CC | Registry | Allocated | AS Name
    """
    asns = [normalize_asn(a) for a in asns]
    asns = sorted({a for a in asns if a})
    if not asns:
        return {}

    query = "begin\nverbose\n" + "\n".join(asns) + "\nend\n"

    result: Dict[str, dict] = {}
    try:
        with socket.create_connection(("whois.cymru.com", 43), timeout=timeout) as s:
            s.settimeout(timeout)
            s.sendall(query.encode("ascii"))

            chunks = []
            while True:
                try:
                    data = s.recv(65536)
                except socket.timeout:
                    break
                if not data:
                    break
                chunks.append(data)

        text = b"".join(chunks).decode("utf-8", errors="replace")
    except Exception as e:
        print(f"WARNING: ASN lookup failed: {e}")
        return {}

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return {}

    for line in lines:
        # Skip header
        if line.upper().startswith("AS "):
            continue

        parts = [p.strip() for p in line.split("|")]
        if len(parts) < 5:
            continue

        asn, cc, registry, allocated, name = parts[0], parts[1], parts[2], parts[3], "|".join(parts[4:]).strip()
        asn = normalize_asn(asn)
        if not asn:
            continue

        result[asn] = {
            "asn_name": name or "Unknown",
            "asn_country": cc or "",
            "registry": registry or "",
            "allocated": allocated or "",
        }

    return result


def add_asn_names(df: pd.DataFrame, cache_path: Path, no_lookup: bool, timeout: int) -> pd.DataFrame:
    out = df.copy()
    out["asn"] = out["asn"].map(normalize_asn)
    out = out[out["asn"] != ""].copy()

    cache = load_cache(cache_path)

    needed = sorted(set(out["asn"]) - set(cache.keys()))
    if needed and not no_lookup:
        print(f"Looking up {len(needed):,} ASN name(s) via Team Cymru...")
        looked_up = team_cymru_bulk_lookup(needed, timeout=timeout)
        for asn in needed:
            cache[asn] = looked_up.get(asn, {
                "asn_name": "Unknown",
                "asn_country": "",
                "registry": "",
                "allocated": "",
            })
        save_cache(cache_path, cache)
    elif needed and no_lookup:
        for asn in needed:
            cache[asn] = {
                "asn_name": "Unknown",
                "asn_country": "",
                "registry": "",
                "allocated": "",
            }

    out["asn_name"] = out["asn"].map(lambda x: cache.get(x, {}).get("asn_name", "Unknown"))
    out["asn_country"] = out["asn"].map(lambda x: cache.get(x, {}).get("asn_country", ""))
    out["registry"] = out["asn"].map(lambda x: cache.get(x, {}).get("registry", ""))
    out["allocated"] = out["asn"].map(lambda x: cache.get(x, {}).get("allocated", ""))

    # Nice final order
    cols = ["asn", "asn_name", "asn_country", "registry", "allocated", "requests", "pct_rows"]
    return out[cols].sort_values("requests", ascending=False).reset_index(drop=True)


def main():
    args = parse_args()

    if args.input:
        print("Building ASN summary from parquet...")
        df = build_summary_from_parquet(args.input, args.asn_col, args.threads)
    else:
        print("Reading ASN summary from CSV...")
        df = build_summary_from_csv(args.csv)

    if args.top and args.top > 0:
        df = df.head(int(args.top)).copy()

    print(f"ASNs found: {len(df):,}")
    out = add_asn_names(df, Path(args.cache), args.no_lookup, args.timeout)

    output_path = Path(args.output)
    out.to_csv(output_path, index=False, encoding="utf-8")
    print()
    print(out.head(50).to_string(index=False))
    print()
    print(f"Saved: {output_path.resolve()}")


if __name__ == "__main__":
    main()
