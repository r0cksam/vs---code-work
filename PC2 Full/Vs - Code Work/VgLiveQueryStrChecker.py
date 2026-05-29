from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

from vglive_core import DEFAULT_LAKE_FOLDER, profile_querystr_channels


def parse_date(value: str | None):
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Profile pure channel names from queryStr and compare them with the VgLive map."
    )
    parser.add_argument("--lake", type=Path, default=DEFAULT_LAKE_FOLDER)
    parser.add_argument("--start", help="Start date, YYYY-MM-DD")
    parser.add_argument("--end", help="End date, YYYY-MM-DD")
    parser.add_argument("--top-n", type=int, default=5000, help="Raw combinations to keep. Use 0 for all.")
    parser.add_argument("--limit", type=int, default=50, help="Rows to print.")
    parser.add_argument("--csv", type=Path, help="Optional CSV output path.")
    parser.add_argument("--ts-only", action="store_true", help="Only inspect .ts chunk rows.")
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
    print(f"Path scope: {'.ts chunks only' if args.ts_only else 'all 200-status paths'}")

    df = profile_querystr_channels(
        lake_path=args.lake,
        start_date=start_date,
        end_date=end_date,
        top_n=args.top_n,
        ts_only=args.ts_only,
    )

    if args.csv:
        args.csv.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(args.csv, index=False)
        print(f"CSV written: {args.csv}")

    if df.empty:
        print("No queryStr channel evidence found.")
        return

    print()
    print(f"Rows profiled: {len(df):,}")
    print(f"Requests: {int(df['requests'].sum()):,}")
    print()

    source_summary = (
        df.groupby("channel_source", dropna=False)["requests"]
        .sum()
        .reset_index()
        .sort_values("requests", ascending=False)
    )
    print("Channel source:")
    for _, row in source_summary.iterrows():
        print(f"  {row['channel_source']:<18} {row['requests']:>12,} req")
    print()

    summary = (
        df.groupby(["review_status", "pure_channel", "mapped_channel"], dropna=False)
        .agg(
            requests=("requests", "sum"),
            sessions=("sessions", "sum"),
            devices=("devices", "sum"),
            unique_viewers=("unique_viewers", "sum"),
        )
        .reset_index()
    )
    status_order = {"unmapped_candidate": 0, "query_mapping_mismatch": 1, "ok": 2}
    summary["_status_order"] = summary["review_status"].map(status_order).fillna(9)
    summary = summary.sort_values(["_status_order", "requests"], ascending=[True, False])

    for _, row in summary.head(args.limit).iterrows():
        print(
            f"{row['review_status']:<24} | "
            f"{row['requests']:>12,} req | "
            f"{row['sessions']:>9,} sessions | "
            f"{row['pure_channel']} -> {row['mapped_channel']}"
        )


if __name__ == "__main__":
    main()
