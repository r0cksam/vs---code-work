from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

from vglive_core import DEFAULT_LAKE_FOLDER, find_unmapped_channels


def parse_date(value: str | None):
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="List VgLive channel candidates that still resolve to Other."
    )
    parser.add_argument("--lake", type=Path, default=DEFAULT_LAKE_FOLDER)
    parser.add_argument("--start", help="Start date, YYYY-MM-DD")
    parser.add_argument("--end", help="End date, YYYY-MM-DD")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--csv", type=Path, help="Optional CSV output path")
    args = parser.parse_args()

    start_date = parse_date(args.start)
    end_date = parse_date(args.end)
    if (start_date is None) != (end_date is None):
        raise SystemExit("Use both --start and --end, or neither.")
    if start_date and start_date > end_date:
        raise SystemExit("--start cannot be after --end.")

    print(f"Lake: {args.lake}")
    if start_date:
        print(f"Date range: {start_date} -> {end_date}")
    else:
        print("Date range: all available dates")

    df = find_unmapped_channels(args.lake, start_date, end_date)
    if args.csv:
        args.csv.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(args.csv, index=False)
        print(f"CSV written: {args.csv}")

    if df.empty:
        print("No unmapped channel candidates found.")
        return

    total_chunks = int(df["dedup_chunks"].sum())
    total_hours = float(df["watch_hours"].sum())
    print()
    print(f"Unmapped candidates : {len(df):,}")
    print(f"Deduped chunks      : {total_chunks:,}")
    print(f"Watch hours         : {total_hours:,.1f}")
    print()

    view = df.head(args.limit).copy()
    for _, row in view.iterrows():
        print(
            f"{row['dedup_chunks']:>12,} chunks | "
            f"{row['watch_hours']:>10.1f} hrs | "
            f"{row['unique_viewers']:>8,} viewers | "
            f"{row['reqHost']} | {row['candidate_id']}"
        )
        print(f"    sample: {row['sample_reqPath']}")


if __name__ == "__main__":
    main()
