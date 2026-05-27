"""
Run this script to find all channel IDs that are currently unmapped (landing in 'Other').
Usage:  python find_unmapped.py
Edit the START_DATE / END_DATE / LAKE_PATH below before running.
"""

import duckdb
from urllib.parse import unquote
from datetime import datetime, timedelta

LAKE_PATH  = r"D:\Veto Logs Backup\veto Stream logs\lake"
START_DATE = datetime(2026, 4, 1)   # ← change these
END_DATE   = datetime(2026, 5, 25)   # ← change these  (same day = single day scan)

# ── Build partition filter for the date range ────────────────────────────────
def build_partition_filter(start, end):
    filters = []
    current = start
    while current <= end:
        filters.append(
            f"(year = {current.year} AND month = {current.month} AND day = {current.day})"
        )
        current += timedelta(days=1)
    return " OR ".join(filters)

partition_filter = build_partition_filter(START_DATE, END_DATE)
start_epoch = int(START_DATE.timestamp())
end_epoch   = int((END_DATE + timedelta(days=1) - timedelta(seconds=1)).timestamp())

QUERY = """
    SELECT
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
                ELSE regexp_replace(
                    regexp_extract(reqPath, '([^/]+)\\.ts$', 1),
                    '[_-]?(1080p?|720p?|480p?|360p?|\\d+)$', ''
                )
            END
        ) AS channel_id,
        COUNT(DISTINCT cliIP) AS unique_viewers,
        COUNT(*)              AS total_chunks,
        any_value(reqPath)    AS sample_reqPath
    FROM read_parquet('{lake}/**/*.parquet', hive_partitioning=1)
    WHERE statusCode = '200'
      AND reqPath LIKE '%.ts'
      AND CAST(reqTimeSec AS DOUBLE) BETWEEN {start_epoch} AND {end_epoch}
      AND ({partition_filter})
    GROUP BY channel_id
    ORDER BY total_chunks DESC
""".format(
    lake=LAKE_PATH.replace("\\", "/"),
    start_epoch=start_epoch,
    end_epoch=end_epoch,
    partition_filter=partition_filter,
)

# ── Channel map (keep in sync with dashboard.py) ─────────────────────────────
CHANNEL_MAP = {
    "vglive-sk-274906": "India TV",
    "vglive-sk-385006": "India TV Yoga",
    "vglive-sk-479089": "India TV SpeedNews",
    "vglive-sk-912213": "India TV Adalat",
    "vglive-sk-699286": "India TV Yoga",
    "vglive-sk-238731": "NDTV Marathi",
    "vglive-sk-639201": "IndiaTV Cricket",
    "vglive-sk-834057": "NDTV India",
    "indiatv": "India TV", "ndtv_india": "NDTV India", "ndtvindia": "NDTV India",
    "ctvndtvindia": "NDTV India", "speednews": "India TV SpeedNews",
    "ndtv_marathi": "NDTV Marathi", "ndtvmarathi": "NDTV Marathi",
    "ctvndtvmarathi": "NDTV Marathi", "b4u_music": "B4U Music", "b4um001": "B4U Music",
    "b4u_movies": "B4U Movies", "b4umo001": "B4U Movies",
    "b4u_kadak": "B4U Kadak", "b4ua001": "B4U Kadak",
    "b4u_bhojpuri": "B4U Bhojpuri",
    "9xm": "9XM", "9xm_jalwa": "9XM Jalwa", "9xjalwa": "9XM Jalwa",
    "9xm_tashan": "9XM Tashan", "9xtashan": "9XM Tashan",
    "9xm_jhakaas": "9XM Jhakaas", "9xjhakaas": "9XM Jhakaas",
    "sanskaartv": "Sanskaar TV", "sanskaar": "Sanskaar TV", "sanskar": "Sanskaar TV",
    "satsanghtv": "Satsangh TV", "satsangh": "Satsangh TV", "satsang": "Satsangh TV",
    "shubhtv": "Shubh TV", "shubh": "Shubh TV",
    "aapkiadalat": "India TV Adalat", "yogatv": "India TV Yoga",
    "manorama": "Manorama", "newsnation": "NewsNation",
    "newsnation_upuk": "NewsNation UP/UK", "nnup": "NewsNation UP/UK",
    "newsnation_pbhr": "NewsNation PB/HR",
    "newsnation_mpch": "NewsNation MP/CH", "nnmp": "NewsNation MP/CH",
    "newsnation_brjh": "NewsNation BR/JH", "nnbrjh": "NewsNation BR/JH",
    "nnpunj": "NewsNation Punjab",
    "gtc_news": "GTC News", "gtcnews": "GTC News",
    "gtc_punjabi": "GTC Punjabi", "gtcpunjabi": "GTC Punjabi",
    "punjabi_shorts": "Punjabi Shorts", "punjabshort": "Punjabi Shorts",
    "bollywoodmasala": "Bollywood Masala",
    "vetocricketlive": "Veto Cricket Live",
    "national": "DD National",
    # ────── added to match dashboard ──────
    "9x_tashan": "9XM Tashan",
    "9x_jhakaas": "9XM Jhakaas",
    "9x_jalwa": "9XM Jalwa",
    "nntv": "DD National",
    "out": "manorama"
}
def resolve(raw):
    if not raw:
        return "Other"
    decoded = unquote(raw).strip().strip("/").lower()
    if decoded in CHANNEL_MAP:
        return CHANNEL_MAP[decoded]
    for key, name in sorted(CHANNEL_MAP.items(), key=lambda x: -len(x[0])):
        if key and key in decoded:
            return name
    return "Other"

# ── Run ──────────────────────────────────────────────────────────────────────
date_range_str = (
    START_DATE.strftime("%Y-%m-%d")
    if START_DATE == END_DATE
    else f"{START_DATE.strftime('%Y-%m-%d')} → {END_DATE.strftime('%Y-%m-%d')}"
)
print(f"Scanning: {date_range_str}")
print("Connecting to lake…")

con = duckdb.connect()
df = con.execute(QUERY).fetchdf()
con.close()

df["resolved"] = df["channel_id"].apply(resolve)
unmapped = df[df["resolved"] == "Other"].copy()
unmapped["watch_hours"] = unmapped["total_chunks"] * (6 / 3600)

print(f"\nTotal distinct channel IDs in data : {len(df)}")
print(f"Unmapped (landing in Other)        : {len(unmapped)}")
print(f"Unmapped chunks                    : {unmapped['total_chunks'].sum():,}")
print(f"Unmapped watch hours               : {unmapped['watch_hours'].sum():,.1f}")
print()

if unmapped.empty:
    print("✅ No unmapped channels! Everything is accounted for.")
else:
    print("=" * 95)
    print(f"{'channel_id':<35} {'viewers':>8} {'chunks':>10} {'watch_hrs':>10}  sample_reqPath")
    print("=" * 95)
    for _, row in unmapped.iterrows():
        print(
            f"{str(row['channel_id']):<35} "
            f"{int(row['unique_viewers']):>8,} "
            f"{int(row['total_chunks']):>10,} "
            f"{row['watch_hours']:>10.1f}  "
            f"{row['sample_reqPath']}"
        )
    print("=" * 95)
    print("\nAdd the channel_id → name pairs above to CHANNEL_MAP in both dashboard.py and this file.")

# ── Also print ALL resolved channels for reference ───────────────────────────
print("\n── All channels found in this date range ──────────────────────────────────────────")
print(f"{'channel_id':<35} {'resolved_name':<25} {'chunks':>10}  sample_reqPath")
print("-" * 95)
for _, row in df.iterrows():
    print(
        f"{str(row['channel_id']):<35} "
        f"{str(row['resolved']):<25} "
        f"{int(row['total_chunks']):>10,}  "
        f"{row['sample_reqPath']}"
    )