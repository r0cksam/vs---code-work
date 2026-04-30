#!/usr/bin/env python
r"""
asn_smart_lookup_ip2location.py
===============================

Resume-safe ASN lookup using IP2Location Lite pages:
    https://lite.ip2location.com/as55836

Designed for large ASN lists like 500 / 1500 / more.

Smart behavior:
- Counts ASNs from CSV or parquet.
- Uses local JSON cache.
- Saves cache immediately after each ASN lookup.
- Writes progress CSV after every batch.
- Skips already cached ASNs on next run.
- Supports sleep between requests.
- Supports adaptive backoff after failures.
- Can stop after max-new-per-run lookups for safe scheduled/batched use.

Example from CSV:
python asn_smart_lookup_ip2location.py `
  --csv "D:\Vs - Code Work\unique_asn.csv" `
  --output "D:\Vs - Code Work\asn_summary_with_ip2location.csv" `
  --top 1500 `
  --sleep 2 `
  --batch-save-every 10

Example safer batch:
python asn_smart_lookup_ip2location.py `
  --csv "D:\Vs - Code Work\unique_asn.csv" `
  --output "D:\Vs - Code Work\asn_summary_with_ip2location.csv" `
  --top 1500 `
  --sleep 3 `
  --max-new-per-run 200

Example from parquet:
python asn_smart_lookup_ip2location.py `
  --input "D:\Vs - Code Work\cleaned_output" `
  --asn-col asn `
  --output "D:\Vs - Code Work\asn_summary_with_ip2location.csv" `
  --threads 10 `
  --top 1500 `
  --sleep 2

Install:
pip install pandas requests duckdb
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
import time
from datetime import datetime
from html import unescape
from pathlib import Path
from typing import Dict, List

import pandas as pd


def parse_args():
    p = argparse.ArgumentParser(description="Resume-safe ASN lookup with IP2Location Lite.")
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--csv", help="Existing ASN count CSV. Example columns: value,count,% of rows")
    src.add_argument("--input", help="Parquet folder/glob/file")

    p.add_argument("--asn-col", default="asn", help="ASN column in parquet. Default: asn")
    p.add_argument("--output", default="asn_summary_with_ip2location.csv", help="Output CSV")
    p.add_argument("--cache", default="ip2location_asn_cache.json", help="Local lookup cache JSON")
    p.add_argument("--threads", type=int, default=6)
    p.add_argument("--top", type=int, default=0, help="Only process top N ASNs. 0 = all")
    p.add_argument("--sleep", type=float, default=2.0, help="Base sleep seconds between requests")
    p.add_argument("--jitter", type=float, default=0.8, help="Random extra sleep 0..jitter seconds")
    p.add_argument("--timeout", type=int, default=25)
    p.add_argument("--batch-save-every", type=int, default=10, help="Write output CSV every N new lookups")
    p.add_argument("--max-new-per-run", type=int, default=0, help="Stop after this many new web lookups. 0 = no limit")
    p.add_argument("--retry", type=int, default=2, help="Retries per ASN")
    p.add_argument("--backoff", type=float, default=30.0, help="Seconds to wait after repeated failure")
    p.add_argument("--refresh-unknown", action="store_true", help="Retry cached Unknown rows")
    p.add_argument("--no-lookup", action="store_true", help="Only merge existing cache with counts; do not request website")
    return p.parse_args()


def normalize_asn(x) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip().upper()
    if s in {"", "NAN", "NONE", "NULL", "(NULL)", "-", "^"}:
        return ""
    if s.startswith("AS"):
        s = s[2:].strip()
    if s.endswith(".0"):
        s = s[:-2]
    s = "".join(ch for ch in s if ch.isdigit())
    return s


def parquet_files(input_path: str) -> List[str]:
    import glob
    p = Path(input_path)
    if p.is_dir():
        files = sorted(str(x) for x in p.glob("*.parquet"))
    elif p.is_file() and p.suffix.lower() == ".parquet":
        files = [str(p)]
    else:
        files = sorted(glob.glob(input_path))
    files = [f for f in files if f.lower().endswith(".parquet")]
    if not files:
        raise FileNotFoundError(f"No parquet files found: {input_path}")
    return files


def build_summary_from_parquet(input_path: str, asn_col: str, threads: int) -> pd.DataFrame:
    try:
        import duckdb
    except ImportError:
        raise RuntimeError("duckdb required for parquet mode. Install: pip install duckdb")

    files = parquet_files(input_path)
    con = duckdb.connect()
    con.execute(f"PRAGMA threads={int(threads)}")
    qcol = '"' + asn_col.replace('"', '""') + '"'

    df = con.execute(f"""
        WITH base AS (
            SELECT
                regexp_replace(
                    regexp_replace(upper(trim(CAST({qcol} AS VARCHAR))), '^AS', ''),
                    '\\.0$', ''
                ) AS asn
            FROM read_parquet(?, union_by_name=true)
        ),
        grouped AS (
            SELECT asn, COUNT(*) AS requests
            FROM base
            WHERE asn IS NOT NULL
              AND asn NOT IN ('', 'NAN', 'NONE', 'NULL', '(NULL)', '-', '^', '0')
            GROUP BY 1
        )
        SELECT
            asn,
            requests,
            ROUND(requests * 100.0 / SUM(requests) OVER (), 4) AS pct_rows
        FROM grouped
        ORDER BY requests DESC
    """, [files]).df()

    df["asn"] = df["asn"].map(normalize_asn)
    df = df[df["asn"] != ""].copy()
    return df


def build_summary_from_csv(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    lower = {c.lower().strip(): c for c in df.columns}

    asn_col = lower.get("asn") or lower.get("value") or df.columns[0]
    count_col = lower.get("requests") or lower.get("count") or df.columns[1]
    pct_col = lower.get("pct_rows") or lower.get("% of rows") or lower.get("percent") or lower.get("percentage")

    out = pd.DataFrame()
    out["asn"] = df[asn_col].map(normalize_asn)
    out["requests"] = pd.to_numeric(df[count_col], errors="coerce").fillna(0).astype("int64")

    if pct_col:
        out["pct_rows"] = pd.to_numeric(df[pct_col], errors="coerce").fillna(0.0)
    else:
        total = out["requests"].sum()
        out["pct_rows"] = (out["requests"] / total * 100).round(4) if total else 0.0

    out = out[out["asn"] != ""].copy()
    return out.sort_values("requests", ascending=False).reset_index(drop=True)


def load_cache(path: Path) -> Dict[str, dict]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"WARNING: could not read cache {path}: {e}")
        return {}


def save_cache(path: Path, cache: Dict[str, dict]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(cache, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def html_to_text(s: str) -> str:
    s = re.sub(r"(?is)<script.*?</script>", " ", s)
    s = re.sub(r"(?is)<style.*?</style>", " ", s)
    s = re.sub(r"(?i)<br\s*/?>", "\n", s)
    s = re.sub(r"(?i)</(p|div|tr|li|h1|h2|h3|td|th)>", "\n", s)
    s = re.sub(r"(?is)<[^>]+>", " ", s)
    s = unescape(s)
    s = re.sub(r"[ \t\r\f\v]+", " ", s)
    s = re.sub(r"\n\s+", "\n", s)
    return s.strip()


def extract_after_label(text: str, label: str) -> str:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    label_low = label.lower().rstrip(":")

    for i, ln in enumerate(lines):
        if ln.lower().rstrip(":") == label_low:
            if i + 1 < len(lines):
                return lines[i + 1].strip()

    pattern = re.compile(rf"{re.escape(label)}\s*:?\s+(.+)", re.IGNORECASE)
    for ln in lines:
        m = pattern.search(ln)
        if m:
            val = m.group(1).strip()
            val = re.split(
                r"\s+(AS Country|AS Number|AS Name|AS Domain Name|ASN Type|Total IPv4 IPs|Total IPv6 IPs)\s*:?\s+",
                val,
                maxsplit=1,
                flags=re.IGNORECASE,
            )[0].strip()
            if val:
                return val
    return ""


def lookup_ip2location(asn: str, timeout: int = 25) -> dict:
    try:
        import requests
    except ImportError:
        raise RuntimeError("requests required. Install: pip install requests")

    asn = normalize_asn(asn)
    url = f"https://lite.ip2location.com/as{asn}"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    resp = requests.get(url, headers=headers, timeout=timeout)

    if resp.status_code in (403, 429, 503):
        raise RuntimeError(f"rate_limited_or_blocked_http_{resp.status_code}")

    resp.raise_for_status()

    text = html_to_text(resp.text)

    as_country = extract_after_label(text, "AS Country")
    as_number = extract_after_label(text, "AS Number")
    as_name = extract_after_label(text, "AS Name")
    as_domain = extract_after_label(text, "AS Domain Name")
    asn_type = extract_after_label(text, "ASN Type")
    total_ipv4 = extract_after_label(text, "Total IPv4 IPs")
    total_ipv6 = extract_after_label(text, "Total IPv6 IPs")

    if not as_name:
        m = re.search(rf"\bAS\s*{re.escape(asn)}\b\s*[-–]?\s*([A-Za-z0-9][^\n|]+)", text, flags=re.IGNORECASE)
        if m:
            as_name = m.group(1).strip()

    return {
        "as_country": as_country or "",
        "as_number": as_number or asn,
        "as_name": as_name or "Unknown",
        "as_domain": as_domain or "",
        "asn_type": asn_type or "",
        "total_ipv4_ips": total_ipv4 or "",
        "total_ipv6_ips": total_ipv6 or "",
        "lookup_source": url,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "ok" if as_name else "unknown",
    }


def merge_output(df: pd.DataFrame, cache: Dict[str, dict]) -> pd.DataFrame:
    out = df.copy()
    out["asn"] = out["asn"].map(normalize_asn)

    def get(asn, key, default=""):
        return cache.get(str(asn), {}).get(key, default)

    out["as_name"] = out["asn"].map(lambda a: get(a, "as_name", "Unknown"))
    out["as_country"] = out["asn"].map(lambda a: get(a, "as_country"))
    out["as_domain"] = out["asn"].map(lambda a: get(a, "as_domain"))
    out["asn_type"] = out["asn"].map(lambda a: get(a, "asn_type"))
    out["total_ipv4_ips"] = out["asn"].map(lambda a: get(a, "total_ipv4_ips"))
    out["total_ipv6_ips"] = out["asn"].map(lambda a: get(a, "total_ipv6_ips"))
    out["lookup_status"] = out["asn"].map(lambda a: get(a, "status", "not_cached"))
    out["updated_at"] = out["asn"].map(lambda a: get(a, "updated_at"))
    out["lookup_source"] = out["asn"].map(lambda a: get(a, "lookup_source"))

    cols = [
        "asn",
        "as_name",
        "as_country",
        "as_domain",
        "asn_type",
        "total_ipv4_ips",
        "total_ipv6_ips",
        "requests",
        "pct_rows",
        "lookup_status",
        "updated_at",
        "lookup_source",
    ]
    return out[cols].sort_values("requests", ascending=False).reset_index(drop=True)


def write_output(df: pd.DataFrame, cache: Dict[str, dict], output: Path) -> None:
    final_df = merge_output(df, cache)
    tmp = output.with_suffix(output.suffix + ".tmp")
    final_df.to_csv(tmp, index=False, encoding="utf-8")
    tmp.replace(output)


def should_lookup(asn: str, cache: Dict[str, dict], refresh_unknown: bool) -> bool:
    rec = cache.get(str(asn))
    if not rec:
        return True
    name = str(rec.get("as_name", "")).strip()
    status = str(rec.get("status", "")).strip()
    if refresh_unknown and (not name or name == "Unknown" or status in {"unknown", "failed"}):
        return True
    return False


def main():
    args = parse_args()

    if args.csv:
        print("Reading ASN counts from CSV...")
        df = build_summary_from_csv(args.csv)
    else:
        print("Building ASN counts from parquet...")
        df = build_summary_from_parquet(args.input, args.asn_col, args.threads)

    if args.top and args.top > 0:
        df = df.head(int(args.top)).copy()

    cache_path = Path(args.cache)
    output_path = Path(args.output)

    cache = load_cache(cache_path)

    total = len(df)
    cached_ok = sum(
        1 for a in df["asn"].tolist()
        if a in cache and str(cache[a].get("as_name", "")).strip() and cache[a].get("as_name") != "Unknown"
    )
    missing = [a for a in df["asn"].tolist() if should_lookup(a, cache, args.refresh_unknown)]

    print(f"ASNs in scope      : {total:,}")
    print(f"Already cached OK  : {cached_ok:,}")
    print(f"Need lookup        : {len(missing):,}")
    print(f"Cache file         : {cache_path.resolve()}")
    print(f"Output CSV         : {output_path.resolve()}")

    # Always write current output first, even before new lookup.
    write_output(df, cache, output_path)

    if args.no_lookup:
        print("No lookup mode enabled. Wrote output using existing cache only.")
        return

    consecutive_failures = 0
    new_done = 0

    for idx, asn in enumerate(df["asn"].tolist(), 1):
        if not should_lookup(asn, cache, args.refresh_unknown):
            continue

        if args.max_new_per_run and new_done >= int(args.max_new_per_run):
            print(f"Reached --max-new-per-run={args.max_new_per_run}. Stopping safely.")
            break

        print(f"[{idx}/{total}] Lookup AS{asn}...")

        success = False
        last_error = ""

        for attempt in range(1, int(args.retry) + 2):
            try:
                rec = lookup_ip2location(asn, timeout=int(args.timeout))
                cache[asn] = rec
                save_cache(cache_path, cache)
                new_done += 1
                consecutive_failures = 0
                success = True
                print(f"  OK: {rec.get('as_name', 'Unknown')}")
                break
            except KeyboardInterrupt:
                print("\nInterrupted by user. Saving output before exit...")
                save_cache(cache_path, cache)
                write_output(df, cache, output_path)
                sys.exit(130)
            except Exception as e:
                last_error = str(e)
                print(f"  Attempt {attempt} failed: {last_error}")

                if "rate_limited_or_blocked" in last_error.lower():
                    wait = max(float(args.backoff), 60.0)
                else:
                    wait = min(float(args.backoff), 10.0) * attempt

                if attempt <= int(args.retry):
                    print(f"  Waiting {wait:.1f}s before retry...")
                    time.sleep(wait)

        if not success:
            consecutive_failures += 1
            cache[asn] = {
                "as_country": "",
                "as_number": asn,
                "as_name": "Unknown",
                "as_domain": "",
                "asn_type": "",
                "total_ipv4_ips": "",
                "total_ipv6_ips": "",
                "lookup_source": f"https://lite.ip2location.com/as{asn}",
                "updated_at": datetime.now().isoformat(timespec="seconds"),
                "status": "failed",
                "error": last_error,
            }
            save_cache(cache_path, cache)
            new_done += 1

            if consecutive_failures >= 5:
                print(f"  {consecutive_failures} failures in a row. Backing off for {float(args.backoff):.1f}s...")
                time.sleep(float(args.backoff))

        # Write progress CSV every N new lookups.
        if new_done % max(int(args.batch_save_every), 1) == 0:
            write_output(df, cache, output_path)
            print(f"  Progress saved after {new_done:,} new lookup(s).")

        sleep_for = max(0.0, float(args.sleep)) + random.random() * max(0.0, float(args.jitter))
        print(f"  Sleeping {sleep_for:.2f}s...")
        time.sleep(sleep_for)

    write_output(df, cache, output_path)
    print()
    print("DONE")
    print(f"New lookups this run: {new_done:,}")
    print(f"Saved: {output_path.resolve()}")
    print()
    preview = pd.read_csv(output_path).head(30)
    print(preview.to_string(index=False))


if __name__ == "__main__":
    main()
