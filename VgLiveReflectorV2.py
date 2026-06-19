from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

from vglive_core import DEFAULT_LAKE_FOLDER, export_unmapped_rows


def parse_date(value: str | None):
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export raw rows for VgLive channel candidates that resolve to Other."
    )
    parser.add_argument("--lake", type=Path, default=DEFAULT_LAKE_FOLDER)
    parser.add_argument("--start", help="Start date, YYYY-MM-DD")
    parser.add_argument("--end", help="End date, YYYY-MM-DD")
    parser.add_argument("--out", type=Path, default=Path("unmapped_raw_rows.csv"))
    parser.add_argument("--max-rows", type=int, default=10000)
    args = parser.parse_args()

    start_date = parse_date(args.start)
    end_date = parse_date(args.end)
    if (start_date is None) != (end_date is None):
        raise SystemExit("Use both --start and --end, or neither.")
    if start_date and start_date > end_date:
        raise SystemExit("--start cannot be after --end.")

    print(f"Lake: {args.lake}")
    print(f"Output: {args.out}")
    if start_date:
        print(f"Date range: {start_date} -> {end_date}")
    else:
        print("Date range: all available dates")
    print(f"Max rows: {'unlimited' if args.max_rows <= 0 else args.max_rows:,}")

    size = export_unmapped_rows(
        lake_path=args.lake,
        output_csv=args.out,
        start_date=start_date,
        end_date=end_date,
        max_rows=args.max_rows,
    )
    print(f"Export complete. File size: {size:,} bytes")


if __name__ == "__main__":
    main()
