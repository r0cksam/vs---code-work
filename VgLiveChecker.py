"""
Run this script to find all channel IDs that are currently unmapped (landing in 'Other').
Usage:  python find_unmapped.py
"""

import duckdb
from urllib.parse import unquote

LAKE_PATH = r"D:\Veto Logs Backup\05 Veto Logs\lake"

# ── Same extraction SQL as the dashboard ────────────────────────────────────
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
        -- grab a sample raw path so you can see the original format
        any_value(reqPath)    AS sample_reqPath
    FROM read_parquet('{lake}/**/*.parquet', hive_partitioning=1)
    WHERE statusCode = '200'
      AND reqPath LIKE '%.ts'
      AND year = 2026 AND month = 5 AND day = 19
    GROUP BY channel_id
    ORDER BY total_chunks DESC
""".format(lake=LAKE_PATH.replace("\\", "/"))

# ── Your current map (paste any additions here too) ─────────────────────────
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
    "ctvndtvmarathi": "NDTV Marathi", "b4u_music": "B4U Music",
    "b4u_movies": "B4U Movies", "b4u_kadak": "B4U Kadak",
    "b4u_bhojpuri": "B4U Bhojpuri", "9xm": "9XM", "9xm_jalwa": "9XM Jalwa",
    "9xjalwa": "9XM Jalwa", "9xm_tashan": "9XM Tashan", "9xtashan": "9XM Tashan",
    "9xm_jhakaas": "9XM Jhakaas", "9xjhakaas": "9XM Jhakaas",
    "sanskaartv": "Sanskaar TV", "sanskaar": "Sanskaar TV", "sanskar": "Sanskaar TV",
    "satsanghtv": "Satsangh TV", "satsangh": "Satsangh TV", "satsang": "Satsangh TV",
    "shubhtv": "Shubh TV", "shubh": "Shubh TV",
    "aapkiadalat": "India TV Adalat", "yogatv": "India TV Yoga",
    "manorama": "Manorama", "newsnation": "NewsNation",
    "newsnation_upuk": "NewsNation UP/UK", "newsnation_pbhr": "NewsNation PB/HR",
    "newsnation_mpch": "NewsNation MP/CH", "newsnation_brjh": "NewsNation BR/JH",
    "gtc_news": "GTC News", "gtcnews": "GTC News",
    "gtc_punjabi": "GTC Punjabi", "gtcpunjabi": "GTC Punjabi",
    "punjabi_shorts": "Punjabi Shorts", "bollywoodmasala": "Bollywood Masala",
    "vetocricketlive": "Veto Cricket Live",
    "1080p": "Other", "out": "Other", "unknown": "Other",
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
print("=" * 90)
print(f"{'channel_id':<35} {'viewers':>8} {'chunks':>10} {'watch_hrs':>10}  sample_reqPath")
print("=" * 90)
for _, row in unmapped.iterrows():
    print(
        f"{str(row['channel_id']):<35} "
        f"{int(row['unique_viewers']):>8,} "
        f"{int(row['total_chunks']):>10,} "
        f"{row['watch_hours']:>10.1f}  "
        f"{row['sample_reqPath']}"
    )
print("=" * 90)
print("\nCopy the channel_id values above and paste them into CHANNEL_MAP in dashboard.py")