"""
Export raw Parquet rows for unmapped (Other) channels – stable version with LIMIT.
Now includes hostname detection for B4U and Manorama.
Usage: python export_unmapped_raw_rows.py
"""

import duckdb
from datetime import datetime, timedelta

# ====================== CONFIGURATION ======================
LAKE_PATH  = r"D:\Veto Logs Backup\veto Stream logs\lake"
START_DATE = datetime(2026, 4, 1)
END_DATE   = datetime(2026, 5, 25)

OUTPUT_CSV = "2todayunmapped_raw_rows.csv"
MAX_ROWS   = 10000

# ====================== CHANNEL MAP ======================
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
    "manorama": "Manorama",   # ← key for host detection result
    "newsnation": "NewsNation",
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
    "9x_tashan": "9XM Tashan",
    "9x_jhakaas": "9XM Jhakaas",
    "9x_jalwa": "9XM Jalwa",
    "nntv": "DD National",
    "1080p": "Other", "720p": "Other", "480p": "Other", "360p": "Other",
    "master_1080": "Other", "master_720": "Other", "master_360": "Other", "master_504": "Other",
    "out": "Other",          # generic 'out' stays as Other – but we will map via hostname first
    "unknown": "Other",
}

# ====================== HELPER ======================
def build_partition_filter(start, end):
    filters = []
    current = start
    while current <= end:
        filters.append(f"(year = {current.year} AND month = {current.month} AND day = {current.day})")
        current += timedelta(days=1)
    return " OR ".join(filters)

def main():
    partition_filter = build_partition_filter(START_DATE, END_DATE)
    start_epoch = int(START_DATE.timestamp())
    end_epoch   = int((END_DATE + timedelta(days=1) - timedelta(seconds=1)).timestamp())

    map_values = ",\n".join(f"    ('{k.lower()}', '{v}')" for k, v in CHANNEL_MAP.items())
    map_sql = f"CREATE TEMP TABLE map AS SELECT * FROM (VALUES\n{map_values}\n) AS t(channel_key, channel_name)"

    limit_clause = f"LIMIT {MAX_ROWS}" if MAX_ROWS > 0 else ""

    query = f"""
    WITH extracted AS (
        SELECT
            *,
            lower(
                CASE
                    -- Hostname detection (highest priority)
                    WHEN reqHost LIKE '%b4u-veto-m%' THEN 'b4u_movies'
                    WHEN reqHost LIKE '%b4u-veto-music%' THEN 'b4u_music'
                    WHEN reqHost LIKE '%b4u-veto-kadak%' THEN 'b4u_kadak'
                    WHEN reqHost LIKE '%manorama-veto%' THEN 'manorama'      -- ← NEW: Manorama detection

                    -- Then vglive-sk IDs
                    WHEN reqPath LIKE '%vglive-sk-%'
                        THEN regexp_extract(reqPath, '(vglive-sk-[0-9]+)', 1)

                    -- Then path-based extraction
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
            ) AS channel_id_raw
        FROM read_parquet('{LAKE_PATH.replace("\\", "/")}/**/*.parquet', hive_partitioning=1)
        WHERE statusCode = '200'
          AND reqPath LIKE '%.ts'
          AND CAST(reqTimeSec AS DOUBLE) BETWEEN {start_epoch} AND {end_epoch}
          AND ({partition_filter})
    )
    SELECT
        e.* EXCLUDE (channel_id_raw),
        e.channel_id_raw,
        COALESCE(m.channel_name, 'Other') AS resolved_name
    FROM extracted e
    LEFT JOIN map m ON e.channel_id_raw = m.channel_key
    WHERE COALESCE(m.channel_name, 'Other') = 'Other'
    {limit_clause}
    """

    print(f"Scanning raw rows from {START_DATE.date()} to {END_DATE.date()} ...")
    con = duckdb.connect()
    
    # === NEW: Enable DuckDB's Native Progress Bar ===
    con.execute("PRAGMA enable_progress_bar;")
    # Optional: adjust how fast it shows up (default is a few seconds, this forces it sooner)
    con.execute("PRAGMA progress_bar_time=100;")
    
    con.execute(map_sql)
    
    print("Executing query and fetching data (this may take a while)...")
    df = con.execute(query).fetchdf() # The terminal will show a dynamic progress bar here!
    
    con.close()

    if df.empty:
        print("No unmapped rows found.")
        return

    df.to_csv(OUTPUT_CSV, index=False)
    print(f"✅ Exported {len(df):,} raw unmapped rows to {OUTPUT_CSV}")
    print(f"Columns (first 5): {list(df.columns)[:5]}")

if __name__ == "__main__":
    main()