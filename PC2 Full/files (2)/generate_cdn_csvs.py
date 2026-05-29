"""
CDN Log → Dashboard CSV Generator
===================================
Supports:  Akamai / Linode CDN logs in .gz or .parquet format
Outputs:   world_stats.csv, india_states.csv, india_cities.csv
Usage:     python generate_cdn_csvs.py --input /path/to/logs --output ./output

Requirements:
    pip install pandas pyarrow fastparquet
"""

import os
import glob
import gzip
import json
import argparse
import urllib.parse
import pandas as pd
import pyarrow.parquet as pq   # FIX #1: use pyarrow schema read — no full load needed

# ── CONFIG ────────────────────────────────────────────────────────────────────

REQUIRED_COLS = [
    'bytes', 'country', 'state', 'city', 'cliIP',
    'statusCode', 'timeToFirstByte', 'throughput',
    'totalBytes', 'reqTimeSec', 'cacheStatus'
]

ERROR_PREFIXES = ('4', '5')

# ── STEP 1: LOAD FILES ────────────────────────────────────────────────────────

def load_gz_file(path: str) -> pd.DataFrame:
    """Load a single Akamai .gz log file (newline-delimited JSON)."""
    records = []
    bad_lines = 0
    with gzip.open(path, 'rt', encoding='utf-8', errors='replace') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                bad_lines += 1  # FIX #2: count bad lines instead of silently dropping

    if bad_lines:
        print(f"  ⚠ Skipped {bad_lines:,} malformed JSON line(s) in {os.path.basename(path)}")

    if not records:
        # Fallback: try reading as CSV inside gz
        with gzip.open(path, 'rt', encoding='utf-8', errors='replace') as f:
            return pd.read_csv(f)

    return pd.DataFrame(records)


def load_parquet_file(path: str) -> pd.DataFrame:
    """
    FIX #1: Load a .parquet file using pyarrow schema introspection
    to get column names without loading all data into memory first.
    """
    pf = pq.ParquetFile(path)
    available_cols = pf.schema_arrow.names          # zero-cost schema read
    cols_to_load = [c for c in REQUIRED_COLS if c in available_cols]
    return pd.read_parquet(path, columns=cols_to_load if cols_to_load else None)


def load_all_files(input_path: str) -> pd.DataFrame:
    """
    Accepts a single .gz or .parquet file, or a folder of them.
    Returns a single combined DataFrame.
    """
    if os.path.isfile(input_path):
        files = [input_path]
    else:
        gz_files      = glob.glob(os.path.join(input_path, '**', '*.gz'),      recursive=True)
        parquet_files = glob.glob(os.path.join(input_path, '**', '*.parquet'), recursive=True)
        files = gz_files + parquet_files

    if not files:
        raise FileNotFoundError(f"No .gz or .parquet files found in: {input_path}")

    print(f"  Found {len(files)} file(s) to process...")

    frames = []
    for f in files:
        print(f"  Loading: {os.path.basename(f)}")
        try:
            if f.endswith('.parquet'):
                df = load_parquet_file(f)
            else:
                df = load_gz_file(f)
            frames.append(df)
        except Exception as e:
            print(f"  ⚠ Skipped {f}: {e}")

    combined = pd.concat(frames, ignore_index=True)
    print(f"  Total rows loaded: {len(combined):,}")
    return combined


# ── STEP 2: CLEAN ─────────────────────────────────────────────────────────────

def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize types, URL-decode geo fields, cast numerics."""

    for col in ('state', 'city'):
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: urllib.parse.unquote(str(x)) if pd.notna(x) else x
            )

    for col in ('bytes', 'totalBytes', 'throughput', 'timeToFirstByte'):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # FIX #4: normalize statusCode — strip whitespace, take first character only
    if 'statusCode' in df.columns:
        df['statusCode'] = (
            df['statusCode']
            .astype(str)
            .str.strip()
            .str[:3]           # keep full 3-digit code, e.g. "404"
        )

    return df


# ── STEP 3: AGGREGATE ────────────────────────────────────────────────────────

def _agg(df: pd.DataFrame, group_cols: list) -> pd.DataFrame:
    """Core aggregation — works for any grouping level."""
    agg_dict = {}

    if 'bytes' in df.columns:
        agg_dict['bandwidth_gb'] = ('bytes', lambda x: round(x.sum() / 1e9, 3))
        agg_dict['requests']     = ('bytes', 'count')

    if 'cliIP' in df.columns:
        agg_dict['unique_viewers'] = ('cliIP', 'nunique')

    if 'statusCode' in df.columns:
        # FIX #4: match on first character of the normalized 3-digit code
        agg_dict['error_rate_pct'] = (
            'statusCode',
            lambda x: round(x.str[0].isin(['4', '5']).mean() * 100, 2)
        )

    # FIX #5: request-weighted average TTFB instead of simple mean of means
    if 'timeToFirstByte' in df.columns and 'bytes' in df.columns:
        # We'll compute weighted sum and divide by total requests per group later.
        # Simpler: use the raw rows — pandas groupby lambda gets the raw series.
        agg_dict['avg_ttfb_ms'] = (
            'timeToFirstByte',
            lambda x: round(
                (x * df.loc[x.index, 'bytes']).sum() /
                df.loc[x.index, 'bytes'].replace(0, pd.NA).sum()
                if df.loc[x.index, 'bytes'].sum() > 0
                else x.mean(),
                1
            )
        )
    elif 'timeToFirstByte' in df.columns:
        agg_dict['avg_ttfb_ms'] = ('timeToFirstByte', lambda x: round(x.mean(), 1))

    if 'throughput' in df.columns:
        agg_dict['avg_throughput_kbps'] = ('throughput', lambda x: round(x.mean(), 1))

    if 'cacheStatus' in df.columns:
        agg_dict['cache_hit_rate_pct'] = (
            'cacheStatus',
            lambda x: round(pd.to_numeric(x, errors='coerce').eq(1).mean() * 100, 2)
        )

    return df.groupby(group_cols).agg(**agg_dict).reset_index()


def build_world_csv(df: pd.DataFrame) -> pd.DataFrame:
    if 'country' not in df.columns:
        raise ValueError("No 'country' column found in data.")
    result = _agg(df, ['country'])
    result.rename(columns={'country': 'country_code'}, inplace=True)
    return result.sort_values('bandwidth_gb', ascending=False)


def build_india_states_csv(df: pd.DataFrame) -> pd.DataFrame:
    if 'state' not in df.columns:
        raise ValueError("No 'state' column found in data.")
    india = df[df['country'] == 'IN'].copy() if 'country' in df.columns else df.copy()
    india = india[india['state'].notna() & (india['state'] != 'nan')]
    result = _agg(india, ['state'])
    result.rename(columns={'state': 'state_name'}, inplace=True)
    return result.sort_values('bandwidth_gb', ascending=False)


def build_india_cities_csv(df: pd.DataFrame) -> pd.DataFrame:
    if 'city' not in df.columns:
        raise ValueError("No 'city' column found in data.")
    # FIX #3: guard against missing 'state' column before filtering on it
    india = df[df['country'] == 'IN'].copy() if 'country' in df.columns else df.copy()
    india = india[india['city'].notna() & (india['city'] != 'nan')]
    if 'state' in india.columns:
        india = india[india['state'].notna() & (india['state'] != 'nan')]
        result = _agg(india, ['state', 'city'])
        result.rename(columns={'state': 'state_name', 'city': 'city_name'}, inplace=True)
    else:
        result = _agg(india, ['city'])
        result.rename(columns={'city': 'city_name'}, inplace=True)
    return result.sort_values('bandwidth_gb', ascending=False)


# ── STEP 4: SAVE ──────────────────────────────────────────────────────────────

def save_csvs(world, states, cities, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)

    world_path  = os.path.join(output_dir, 'world_stats.csv')
    states_path = os.path.join(output_dir, 'india_states.csv')
    cities_path = os.path.join(output_dir, 'india_cities.csv')

    world.to_csv(world_path,  index=False)
    states.to_csv(states_path, index=False)
    cities.to_csv(cities_path, index=False)

    print(f"\n  ✓ world_stats.csv    → {len(world)} countries")
    print(f"  ✓ india_states.csv   → {len(states)} states")
    print(f"  ✓ india_cities.csv   → {len(cities)} cities")
    print(f"\n  Output folder: {os.path.abspath(output_dir)}")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Generate CDN geo dashboard CSVs')
    parser.add_argument('--input',  required=True,
                        help='Path to a .gz / .parquet file, or a folder of them')
    parser.add_argument('--output', default='./cdn_output',
                        help='Folder to write CSVs into (default: ./cdn_output)')
    args = parser.parse_args()

    print("\n── CDN CSV Generator ────────────────────────────────")
    print(f"  Input : {args.input}")
    print(f"  Output: {args.output}")
    print("─────────────────────────────────────────────────────\n")

    print("[1/4] Loading files...")
    df = load_all_files(args.input)

    print("\n[2/4] Cleaning data...")
    df = clean(df)

    print("\n[3/4] Aggregating...")
    world  = build_world_csv(df)
    states = build_india_states_csv(df)
    cities = build_india_cities_csv(df)

    print("\n[4/4] Saving CSVs...")
    save_csvs(world, states, cities, args.output)

    print("\nDone! Load these CSVs into the dashboard HTML.\n")


if __name__ == '__main__':
    main()
