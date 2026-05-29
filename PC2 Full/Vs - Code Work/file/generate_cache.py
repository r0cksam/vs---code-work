"""
generate_cache.py  —  VgLive Dashboard Cache Generator
=======================================================
Run this ONCE (or on a schedule) to pre-aggregate your raw parquet lake
into three small parquet files.  The dashboard then reads these instead
of re-scanning terabytes every time.

Usage:
    python generate_cache.py
    python generate_cache.py --start 2026-01-01 --end 2026-05-31
    python generate_cache.py --lake "D:/MyLake" --out "D:/cache"

Output files (in --out folder):
    channel_summary.parquet   — per-channel metrics (feeds cards + KPI row)
    top_users.parquet         — top 20 000 viewer rows (feeds viewer table)
    unmapped_ids.parquet      — raw IDs that fell to "Other" (debug expander)
    meta.json                 — date range + generation timestamp
"""

import argparse
import json
import sys
from datetime import datetime, time, timedelta
from pathlib import Path
from urllib.parse import unquote

import duckdb
import pandas as pd

# ====================== CONFIG (edit defaults here) ======================
DEFAULT_LAKE   = Path(r"D:\Veto Logs Backup\veto Stream logs\lake")
DEFAULT_OUT    = Path(r"D:\Veto Logs Backup\veto Stream logs\dashboard_cache")
CHUNK_DURATION_HOURS = 6 / 3600.0

# ====================== CHANNEL MAP ======================
CHANNEL_MAP_RAW = {
    "vglive-sk-274906": "India TV",
    "vglive-sk-385006": "India TV Yoga",
    "vglive-sk-479089": "India TV SpeedNews",
    "vglive-sk-912213": "India TV Adalat",
    "vglive-sk-699286": "India TV Yoga",
    "vglive-sk-238731": "NDTV Marathi",
    "vglive-sk-639201": "IndiaTV Cricket",
    "vglive-sk-834057": "NDTV India",
    "indiatv": "India TV", "india%20tv": "India TV", "india tv": "India TV",
    "speednews": "India TV SpeedNews", "aapkiadalat": "India TV Adalat",
    "yogatv": "India TV Yoga", "yoga": "India TV Yoga",
    "indiatvcrk": "IndiaTV Cricket",
    "ndtv_india": "NDTV India", "ndtvindia": "NDTV India",
    "ctvndtvindia": "NDTV India", "ndtv india": "NDTV India",
    "ndtv_marathi": "NDTV Marathi", "ndtvmarathi": "NDTV Marathi",
    "ctvndtvmarathi": "NDTV Marathi",
    "b4u_music": "B4U Music", "b4umusic": "B4U Music", "b4um001": "B4U Music",
    "b4u_movies": "B4U Movies", "b4umovies": "B4U Movies", "b4umo001": "B4U Movies",
    "b4u_kadak": "B4U Kadak", "b4ukadak": "B4U Kadak", "b4ua001": "B4U Kadak",
    "b4u_bhojpuri": "B4U Bhojpuri", "b4ubhojpuri": "B4U Bhojpuri",
    "b4u bhojpuri": "B4U Bhojpuri",
    "9xm": "9XM", "9xm_jalwa": "9XM Jalwa", "9xmjalwa": "9XM Jalwa",
    "9xjalwa": "9XM Jalwa", "9xm_tashan": "9XM Tashan", "9xmtashan": "9XM Tashan",
    "9xtashan": "9XM Tashan", "9xm_jhakaas": "9XM Jhakaas",
    "9xmjhakaas": "9XM Jhakaas", "9xjhakaas": "9XM Jhakaas",
    "newsnation": "NewsNation", "news nation": "NewsNation",
    "newsnation_upuk": "NewsNation UP/UK", "newsnation upuk": "NewsNation UP/UK",
    "nnup": "NewsNation UP/UK", "newsnation_pbhr": "NewsNation PB/HR",
    "newsnation_mpch": "NewsNation MP/CH", "nnmp": "NewsNation MP/CH",
    "newsnation_brjh": "NewsNation BR/JH", "nnbrjh": "NewsNation BR/JH",
    "nnpunj": "NewsNation Punjab",
    "gtc_news": "GTC News", "gtcnews": "GTC News",
    "gtc_punjabi": "GTC Punjabi", "gtcpunjabi": "GTC Punjabi",
    "sanskaartv": "Sanskaar TV", "sanskaar": "Sanskaar TV", "sanskar": "Sanskaar TV",
    "satsanghtv": "Satsangh TV", "satsangh": "Satsangh TV", "satsang": "Satsangh TV",
    "shubhtv": "Shubh TV", "shubh": "Shubh TV",
    "manorama": "Manorama", "national": "DD National",
    "punjabi_shorts": "Punjabi Shorts", "punjabshort": "Punjabi Shorts",
    "bollywoodmasala": "Bollywood Masala", "bollywood masala": "Bollywood Masala",
    "vetocricketlive": "Veto Cricket Live",
    "1080p": "Other", "out": "Other", "unknown": "Other",
}
CHANNEL_MAP = {k.lower(): v for k, v in CHANNEL_MAP_RAW.items()}


def resolve_channel(raw_id: str) -> str:
    if not raw_id or not isinstance(raw_id, str):
        return "Other"
    decoded = unquote(raw_id).strip().strip("/").strip()
    lower   = decoded.lower()
    if lower in CHANNEL_MAP:
        return CHANNEL_MAP[lower]
    best, best_len = None, 0
    for key, name in CHANNEL_MAP.items():
        if lower.startswith(key) and len(key) > best_len:
            best, best_len = name, len(key)
    if best:
        return best
    for key, name in sorted(CHANNEL_MAP.items(), key=lambda x: -len(x[0])):
        if key and key in lower:
            return name
    return "Other"


def build_partition_filter(start_date, end_date):
    filters, current = [], start_date
    while current <= end_date:
        filters.append(
            f"(year = {current.year} AND month = {current.month} AND day = {current.day})"
        )
        current += timedelta(days=1)
    return " OR ".join(filters) if filters else "1=1"


def get_available_dates(lake_path: Path):
    dates = []
    for year_dir in lake_path.glob("year=*"):
        try:
            year = int(year_dir.name.split("=")[1])
            for month_dir in year_dir.glob("month=*"):
                month = int(month_dir.name.split("=")[1])
                for day_dir in month_dir.glob("day=*"):
                    day = int(day_dir.name.split("=")[1])
                    dates.append(datetime(year, month, day))
        except (ValueError, IndexError):
            continue
    if dates:
        return min(dates).date(), max(dates).date()
    return None, None


def generate_cache(lake_path: Path, out_path: Path, start_date, end_date):
    out_path.mkdir(parents=True, exist_ok=True)
    print(f"\n📂 Lake   : {lake_path}")
    print(f"📦 Output : {out_path}")
    print(f"📅 Range  : {start_date} → {end_date}\n")

    start_epoch      = int(datetime.combine(start_date, time.min).timestamp())
    end_epoch        = int(datetime.combine(end_date,   time.max).timestamp())
    partition_filter = build_partition_filter(start_date, end_date)

    con = duckdb.connect()
    try:
        print("⏳ Step 1/4  Scanning lake and building base table…")
        con.execute(f"""
            CREATE TEMP TABLE base_tmp AS
            SELECT
                cliIP,
                lower(
                    CASE
                        WHEN reqPath LIKE '%vglive-sk-%'
                            THEN regexp_extract(reqPath, '(vglive-sk-[0-9]+)', 1)
                        WHEN reqPath LIKE '%/%' THEN
                            CASE
                                WHEN lower(split_part(ltrim(reqPath, '/'), '/', 1))
                                     IN ('v1', 'live', 'stream', 'hls', '')
                                    THEN split_part(ltrim(reqPath, '/'), '/', 2)
                                ELSE split_part(ltrim(reqPath, '/'), '/', 1)
                            END
                        ELSE
                            regexp_replace(
                                regexp_extract(reqPath, '([^/]+)\\.ts$', 1),
                                '[_-]?(1080p?|720p?|480p?|360p?|\\d+)$', ''
                            )
                    END
                ) AS channel_id
            FROM read_parquet('{lake_path.as_posix()}/**/*.parquet', hive_partitioning=1)
            WHERE statusCode = '200'
              AND reqPath LIKE '%.ts'
              AND CAST(reqTimeSec AS DOUBLE) BETWEEN {start_epoch} AND {end_epoch}
              AND ({partition_filter})
        """)

        print("⏳ Step 2/4  Aggregating raw channel stats…")
        raw_channel_df = con.execute("""
            SELECT
                channel_id,
                COUNT(DISTINCT cliIP) AS unique_viewers,
                COUNT(*)              AS total_chunks
            FROM base_tmp
            GROUP BY channel_id
        """).fetchdf()

        print("⏳ Step 3/4  Aggregating top users…")
        user_df = con.execute("""
            SELECT
                channel_id,
                cliIP,
                COUNT(*) AS chunks_watched
            FROM base_tmp
            GROUP BY channel_id, cliIP
            ORDER BY chunks_watched DESC
            LIMIT 20000
        """).fetchdf()

        con.execute("DROP TABLE base_tmp")

    finally:
        con.close()

    if raw_channel_df.empty:
        print("❌ No data found for this range. Cache NOT written.")
        sys.exit(1)

    print("⏳ Step 4/4  Resolving channel names and writing parquet files…")

    # ── Resolve names ──────────────────────────────────────────────────────
    raw_channel_df["channel_name"] = raw_channel_df["channel_id"].apply(resolve_channel)
    user_df["channel_name"]        = user_df["channel_id"].apply(resolve_channel)

    # ── unmapped_ids.parquet ───────────────────────────────────────────────
    unmapped_df = raw_channel_df[raw_channel_df["channel_name"] == "Other"].copy()
    unmapped_df["watch_hours"] = unmapped_df["total_chunks"] * CHUNK_DURATION_HOURS
    unmapped_df = unmapped_df.sort_values("total_chunks", ascending=False).reset_index(drop=True)
    unmapped_df.to_parquet(out_path / "unmapped_ids.parquet", index=False)
    print(f"   ✅ unmapped_ids.parquet   ({len(unmapped_df):,} rows)")

    # ── channel_summary.parquet ───────────────────────────────────────────
    channel_df = raw_channel_df.groupby("channel_name", as_index=False).agg(
        unique_viewers=("unique_viewers", "sum"),
        total_chunks=("total_chunks",   "sum"),
    )
    channel_df["watch_hours"]                = channel_df["total_chunks"] * CHUNK_DURATION_HOURS
    channel_df["avg_chunks_per_viewer"]      = channel_df["total_chunks"] / channel_df["unique_viewers"]
    channel_df["avg_watch_hours_per_viewer"] = channel_df["watch_hours"]  / channel_df["unique_viewers"]
    channel_df = channel_df.sort_values("watch_hours", ascending=False).reset_index(drop=True)
    channel_df.to_parquet(out_path / "channel_summary.parquet", index=False)
    print(f"   ✅ channel_summary.parquet ({len(channel_df):,} channels)")

    # ── top_users.parquet ─────────────────────────────────────────────────
    user_df["watch_hours"] = user_df["chunks_watched"] * CHUNK_DURATION_HOURS
    user_df = user_df.drop(columns=["channel_id"]).reset_index(drop=True)
    user_df.to_parquet(out_path / "top_users.parquet", index=False)
    print(f"   ✅ top_users.parquet       ({len(user_df):,} rows)")

    # ── meta.json ─────────────────────────────────────────────────────────
    meta = {
        "generated_at": datetime.now().isoformat(),
        "start_date":   str(start_date),
        "end_date":     str(end_date),
        "lake_path":    str(lake_path),
        "total_channels": len(channel_df[channel_df["channel_name"] != "Other"]),
        "total_watch_hours": round(float(channel_df["watch_hours"].sum()), 2),
        "total_unique_viewers": int(channel_df["unique_viewers"].sum()),
        "total_chunks": int(channel_df["total_chunks"].sum()),
    }
    (out_path / "meta.json").write_text(json.dumps(meta, indent=2))
    print(f"   ✅ meta.json")

    print(f"\n🎉 Done! Cache written to: {out_path}")
    print(f"   Channels  : {meta['total_channels']}")
    print(f"   Watch hrs : {meta['total_watch_hours']:,.1f}")
    print(f"   Viewers   : {meta['total_unique_viewers']:,}")


# ====================== CLI ======================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate VgLive dashboard cache")
    parser.add_argument("--lake",  default=str(DEFAULT_LAKE),  help="Path to parquet lake folder")
    parser.add_argument("--out",   default=str(DEFAULT_OUT),   help="Output folder for cache files")
    parser.add_argument("--start", default=None, help="Start date YYYY-MM-DD (default: auto-detect)")
    parser.add_argument("--end",   default=None, help="End date YYYY-MM-DD (default: auto-detect)")
    args = parser.parse_args()

    lake_path = Path(args.lake)
    out_path  = Path(args.out)

    if not lake_path.exists():
        print(f"❌ Lake folder not found: {lake_path}")
        sys.exit(1)

    if args.start and args.end:
        start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
        end_date   = datetime.strptime(args.end,   "%Y-%m-%d").date()
    else:
        print("🔍 Auto-detecting date range from lake…")
        start_date, end_date = get_available_dates(lake_path)
        if not start_date:
            print("❌ Could not detect dates. Pass --start and --end manually.")
            sys.exit(1)
        print(f"   Detected: {start_date} → {end_date}")

    generate_cache(lake_path, out_path, start_date, end_date)
