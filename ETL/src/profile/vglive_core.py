from __future__ import annotations

import re
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from urllib.parse import unquote, unquote_plus
import os

import duckdb
import pandas as pd


ETL_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_LAKE_FOLDER = ETL_ROOT / "data" / "lake"
env_lake = os.getenv("VG_ETL_LAKE_ROOT")
if env_lake:
    DEFAULT_LAKE_FOLDER = Path(env_lake)
else:
    for candidate in [
        DEFAULT_LAKE_FOLDER,
        ETL_ROOT / "lake",
    ]:
        if candidate.exists():
            DEFAULT_LAKE_FOLDER = candidate
            break
CHUNK_DURATION_SECONDS = 6
CHUNK_DURATION_HOURS = CHUNK_DURATION_SECONDS / 3600.0
IST = timezone(timedelta(hours=5, minutes=30))


HOST_MAP = {
    "manorama-veto.akamaized.net": "Manorama",
    "b4u-veto-m.akamaized.net": "B4U Movies",
    "b4u-veto-music.akamaized.net": "B4U Music",
    "b4u-veto-kadak.akamaized.net": "B4U Kadak",
    "b4u-veto.akamaized.net": "B4U Bhojpuri",
    "vetocricket.akamaized.net": "Veto Cricket Live",
    "bmasala-live.akamaized.net": "Bollywood Masala",
}


PATH_MAP = {
    # User-approved VgLive stream IDs.
    "vglive-sk-238731": "NDTV Marathi",
    "vglive-sk-639201": "IndiaTV Cricket",
    "vglive-sk-834057": "Ndtv India",
    "vglive-sk-274906": "India TV",
    "vglive-sk-385006": "India TV Yoga",
    "vglive-sk-479089": "India TV SpeedNews",
    "vglive-sk-912213": "India TV Adalat",
    "vglive-sk-699286": "India TV Yoga",
    "speednews": "India TV SpeedNews",
    "rimo": "India TV SpeedNews",

    # High-confidence path IDs observed in the lake.
    "national": "NewsNation",
    "nnup": "NewsNation UP/UK",
    "nnmp": "NewsNation MP/CH",
    "nnbrjh": "NewsNation BR/JH",
    "nnpunj": "NewsNation Punjab",
    "sanskar": "Sanskaar TV",
    "sanskaartv": "Sanskaar TV",
    "satsang": "Satsangh TV",
    "satsanghtv": "Satsangh TV",
    "shubh": "Shubh TV",
    "shubhtv": "Shubh TV",
    "9xm": "9XM",
    "9xjalwa": "9XM Jalwa",
    "9xm_jalwa": "9XM Jalwa",
    "9x_jalwa": "9XM Jalwa",
    "9xtashan": "9XM Tashan",
    "9xm_tashan": "9XM Tashan",
    "9x_tashan": "9XM Tashan",
    "9xjhakaas": "9XM Jhakaas",
    "9xm_jhakaas": "9XM Jhakaas",
    "9x_jhakaas": "9XM Jhakaas",
    "gtcnews": "GTC News",
    "gtc_news": "GTC News",
    "gtcpunjabi": "GTC Punjabi",
    "gtc_punjabi": "GTC Punjabi",
    "punjabshort": "Punjabi Shorts",
    "punjabi_shorts": "Punjabi Shorts",
    "b4umo001": "B4U Movies",
    "b4um001": "B4U Music",
    "b4ua001": "B4U Kadak",
    "b4u_bhojpuri": "B4U Bhojpuri",
    "bollywoodmasala": "Bollywood Masala",
    "vetocricketlive": "Veto Cricket Live",
}


QUERY_CHANNEL_ALIASES = {
    "speednews": "India TV SpeedNews",
    "yogatv": "India TV Yoga",
    "aapkiadalat": "India TV Adalat",
}


ARTIFACT_IDS = {
    "out",
    "1080p",
    "720p",
    "480p",
    "360p",
    "master_1080",
    "master_720",
    "master_504",
    "master_360",
    "unknown",
}


SKIP_PATH_SEGMENTS = {"v1", "live", "stream", "hls", "nntv", ""}


DEVICE_SUFFIX_TOKENS = {
    "firetv",
    "firestick",
    "fireos",
    "androidtv",
    "android",
    "web",
    "webos",
    "lg",
    "lgtv",
    "apple",
    "appletv",
    "ios",
    "iphone",
    "ipad",
    "samsung",
    "samsungtv",
    "tizen",
    "roku",
    "mi",
    "mitv",
    "xiaomi",
    "sony",
    "bravia",
    "id",
    "tv",
    "mobile",
    "phone",
    "tablet",
}


def _sql_path(path: Path) -> str:
    return path.as_posix().replace("'", "''")


def parquet_glob(lake_path: str | Path) -> str:
    return _sql_path(Path(lake_path) / "**" / "*.parquet")


def normalize_token(value: object) -> str:
    if value is None:
        return ""
    return unquote(str(value)).strip().strip("/").strip().lower()


def _norm_device_token(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return re.sub(r"[^a-z0-9]+", "", str(value).strip().lower())


def _expand_device_tokens(*values: object) -> set[str]:
    expanded = set(DEVICE_SUFFIX_TOKENS)
    for value in values:
        token = _norm_device_token(value)
        if not token:
            continue
        expanded.add(token)
        if token.endswith("tv") and len(token) > 2:
            expanded.add(token[:-2])
        if token == "androidtv":
            expanded.add("android")
        if token == "appletv":
            expanded.add("apple")
        if token == "lgtv":
            expanded.add("lg")
        if token == "samsungtv":
            expanded.add("samsung")
        if token == "firestick":
            expanded.add("firetv")
    return expanded


def normalize_channel_name_smart(channel_raw: object, platform: object = "", device_name: object = "") -> str:
    """Return the pDash-style pure channel name from queryStr evidence."""
    if channel_raw is None or pd.isna(channel_raw):
        return "unknown"

    raw = unquote_plus(str(channel_raw)).strip()
    if raw.lower() in {"", "nan", "none", "null", "(null)"}:
        return "unknown"

    raw = re.sub(r"\s*_\s*", "_", raw)
    raw = re.sub(r"\s+", " ", raw).strip()

    clean = raw
    device_tokens = _expand_device_tokens(platform, device_name)
    while "_" in clean:
        base, suffix = clean.rsplit("_", 1)
        if _norm_device_token(suffix) in device_tokens:
            clean = base.strip("_ ").strip()
            continue
        break

    return (clean or "unknown").casefold()


def resolve_channel(req_host: object, candidate_id: object) -> str:
    host = normalize_token(req_host)
    candidate = normalize_token(candidate_id)

    if host in HOST_MAP:
        return HOST_MAP[host]
    if candidate in PATH_MAP:
        return PATH_MAP[candidate]
    if candidate in ARTIFACT_IDS:
        return "Other"
    return "Other"


def channel_candidate_sql(req_path_col: str = "reqPath") -> str:
    skip_list = ", ".join(f"'{x}'" for x in sorted(SKIP_PATH_SEGMENTS))
    return f"""
lower(
    CASE
        WHEN {req_path_col} LIKE '%vglive-sk-%'
            THEN regexp_extract({req_path_col}, '(vglive-sk-[0-9]+)', 1)
        WHEN {req_path_col} LIKE '%/%' THEN
            CASE
                WHEN lower(split_part(ltrim({req_path_col}, '/'), '/', 1)) IN ({skip_list})
                    THEN split_part(ltrim({req_path_col}, '/'), '/', 2)
                ELSE split_part(ltrim({req_path_col}, '/'), '/', 1)
            END
        ELSE regexp_replace(
            regexp_extract({req_path_col}, '([^/]+)\\.ts$', 1),
            '[_-]?(1080p?|720p?|480p?|360p?|\\d+)$',
            ''
        )
    END
)
"""


def _query_param_sql(param_name: str, query_col: str = "queryStr") -> str:
    return f"regexp_extract({query_col}, '(?i)(?:^|[?&]){param_name}=([^&]+)', 1)"


def build_partition_filter(start_date, end_date) -> str:
    if start_date is None or end_date is None:
        return "1=1"

    filters = []
    current = start_date
    while current <= end_date:
        filters.append(f"(year = {current.year} AND month = '{current.month:02d}' AND day = '{current.day:02d}')")
        current += timedelta(days=1)
    return f"({' OR '.join(filters)})" if filters else "1=1"


def _register_mapping_tables(con: duckdb.DuckDBPyConnection) -> None:
    host_df = pd.DataFrame(
        [{"reqHost": host, "host_channel_name": name} for host, name in HOST_MAP.items()]
    )
    path_df = pd.DataFrame(
        [{"candidate_id": candidate, "path_channel_name": name} for candidate, name in PATH_MAP.items()]
    )

    con.register("host_map_df", host_df)
    con.register("path_map_df", path_df)
    con.execute("CREATE OR REPLACE TEMP TABLE host_map AS SELECT * FROM host_map_df")
    con.execute("CREATE OR REPLACE TEMP TABLE path_map AS SELECT * FROM path_map_df")


def get_available_dates(lake_path: str | Path):
    lake_path = Path(lake_path)
    dates = []
    for day_dir in lake_path.glob("**/day=*"):
        try:
            parts = {
                piece.split("=", 1)[0]: piece.split("=", 1)[1]
                for piece in day_dir.parts
                if "=" in piece
            }
            year = int(parts["year"])
            month = int(parts["month"])
            day = int(parts["day"])
            dates.append(datetime(year, month, day))
        except (KeyError, ValueError, IndexError):
            continue

    if dates:
        return min(dates), max(dates)

    files = list(lake_path.glob("**/*.parquet"))
    if not files:
        return None, None

    glob = parquet_glob(lake_path)
    con = duckdb.connect()
    try:
        result = con.execute(f"""
            SELECT MIN(CAST(reqTimeSec AS DOUBLE)), MAX(CAST(reqTimeSec AS DOUBLE))
            FROM read_parquet('{glob}', hive_partitioning=1)
        """).fetchone()
        if result and result[0] is not None and result[1] is not None:
            return (
                datetime.fromtimestamp(float(result[0]), IST).replace(tzinfo=None),
                datetime.fromtimestamp(float(result[1]), IST).replace(tzinfo=None),
            )
    finally:
        con.close()
    return None, None


def inspect_lake(lake_path: str | Path) -> dict:
    lake_path = Path(lake_path)
    if not list(lake_path.glob("**/*.parquet")):
        return {"error": "No parquet files found in the lake folder."}

    glob = parquet_glob(lake_path)
    con = duckdb.connect()
    try:
        total_rows = con.execute(f"""
            SELECT SUM(row_group_num_rows) AS total_rows
            FROM (
                SELECT DISTINCT file_name, row_group_id, row_group_num_rows
                FROM parquet_metadata('{glob}')
            )
        """).fetchone()[0]
        valid_ts = con.execute(f"""
            SELECT COUNT(*)
            FROM read_parquet('{glob}', hive_partitioning=1)
            WHERE statusCode = '200' AND reqPath LIKE '%.ts'
        """).fetchone()[0]
        return {"total_rows": int(total_rows or 0), "valid_ts_rows": int(valid_ts or 0)}
    except Exception as exc:
        return {"error": str(exc)}
    finally:
        con.close()


def _date_where(start_date, end_date) -> tuple[str, int | None, int | None]:
    if start_date is None or end_date is None:
        return "1=1", None, None
    start_epoch = int(datetime.combine(start_date, time.min, tzinfo=IST).timestamp())
    end_epoch = int(datetime.combine(end_date, time.max, tzinfo=IST).timestamp())
    return "CAST(reqTimeSec AS DOUBLE) BETWEEN ? AND ?", start_epoch, end_epoch


def _create_resolved_table(con: duckdb.DuckDBPyConnection, glob: str, start_date, end_date) -> None:
    partition_filter = build_partition_filter(start_date, end_date)
    date_where, start_epoch, end_epoch = _date_where(start_date, end_date)
    candidate_expr = channel_candidate_sql("reqPath")

    params = []
    if start_epoch is not None and end_epoch is not None:
        params = [start_epoch, end_epoch]

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE base_tmp AS
        SELECT
            cliIP,
            lower(reqHost) AS reqHost,
            {candidate_expr} AS candidate_id,
            reqPath,
            reqTimeSec
        FROM read_parquet('{glob}', hive_partitioning=1)
        WHERE statusCode = '200'
          AND reqPath LIKE '%.ts'
          AND ({date_where})
          AND ({partition_filter})
    """, params)

    _register_mapping_tables(con)

    con.execute("""
        CREATE OR REPLACE TEMP TABLE resolved_tmp AS
        SELECT
            b.cliIP,
            b.reqHost,
            b.candidate_id,
            b.reqPath,
            b.reqTimeSec,
            COALESCE(h.host_channel_name, p.path_channel_name, 'Other') AS channel_name
        FROM base_tmp b
        LEFT JOIN host_map h ON b.reqHost = h.reqHost
        LEFT JOIN path_map p ON b.candidate_id = p.candidate_id
    """)


def compute_metrics(lake_path: str | Path, start_date, end_date, user_limit: int = 20000):
    glob = parquet_glob(lake_path)
    con = duckdb.connect()
    con.execute("PRAGMA threads=8")
    try:
        _create_resolved_table(con, glob, start_date, end_date)

        con.execute("""
            CREATE OR REPLACE TEMP TABLE dedup_tmp AS
            SELECT DISTINCT cliIP, channel_name, reqPath
            FROM resolved_tmp
        """)

        channel_df = con.execute("""
            SELECT
                channel_name,
                COUNT(DISTINCT cliIP) AS unique_viewers,
                COUNT(*) AS total_chunks
            FROM dedup_tmp
            GROUP BY channel_name
            ORDER BY total_chunks DESC
        """).fetchdf()

        user_df = con.execute(f"""
            SELECT
                channel_name,
                cliIP,
                COUNT(*) AS chunks_watched
            FROM dedup_tmp
            GROUP BY channel_name, cliIP
            ORDER BY chunks_watched DESC
            LIMIT {int(user_limit)}
        """).fetchdf()

        raw_channel_df = con.execute("""
            SELECT
                reqHost,
                candidate_id AS channel_id,
                channel_name,
                COUNT(*) AS raw_chunks,
                COUNT(DISTINCT coalesce(cliIP, '') || '|' || channel_name || '|' || coalesce(reqPath, '')) AS total_chunks,
                COUNT(DISTINCT cliIP) AS unique_viewers
            FROM resolved_tmp
            GROUP BY reqHost, candidate_id, channel_name
            ORDER BY total_chunks DESC
        """).fetchdf()

        if channel_df.empty:
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), "No data found."

        channel_df["watch_hours"] = channel_df["total_chunks"] * CHUNK_DURATION_HOURS
        channel_df["avg_chunks_per_viewer"] = channel_df["total_chunks"] / channel_df["unique_viewers"]
        channel_df["avg_watch_hours_per_viewer"] = channel_df["watch_hours"] / channel_df["unique_viewers"]
        channel_df = channel_df.sort_values("watch_hours", ascending=False).reset_index(drop=True)

        user_df["watch_hours"] = user_df["chunks_watched"] * CHUNK_DURATION_HOURS

        raw_channel_df["watch_hours"] = raw_channel_df["total_chunks"] * CHUNK_DURATION_HOURS
        unmapped_df = raw_channel_df[raw_channel_df["channel_name"] == "Other"].copy()
        unmapped_df = unmapped_df.sort_values("total_chunks", ascending=False)

        if start_date is None or end_date is None:
            time_range = "All available dates"
        else:
            time_range = f"{start_date.strftime('%Y-%m-%d')} -> {end_date.strftime('%Y-%m-%d')}"

        return channel_df, user_df, unmapped_df, raw_channel_df, time_range
    finally:
        con.close()


def _compact_channel_name(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", "", normalize_channel_name_smart(value))


def profile_querystr_channels(
    lake_path: str | Path,
    start_date=None,
    end_date=None,
    top_n: int = 5000,
    ts_only: bool = False,
) -> pd.DataFrame:
    """Profile pDash-style pure channel names from queryStr without changing mappings."""
    glob = parquet_glob(lake_path)
    con = duckdb.connect()
    con.execute("PRAGMA threads=8")

    partition_filter = build_partition_filter(start_date, end_date)
    date_where, start_epoch, end_epoch = _date_where(start_date, end_date)
    candidate_expr = channel_candidate_sql("reqPath")
    channel_qs = _query_param_sql("channel")
    channel_name_qs = _query_param_sql("channel_name")
    platform_qs = _query_param_sql("platform")
    device_qs = _query_param_sql("device")
    session_qs = _query_param_sql("session_id")
    device_id_qs = _query_param_sql("device_id")
    limit_clause = "" if top_n <= 0 else f"LIMIT {int(top_n)}"
    path_filter = "AND reqPath LIKE '%.ts'" if ts_only else ""
    params = []
    if start_epoch is not None and end_epoch is not None:
        params = [start_epoch, end_epoch]

    try:
        df = con.execute(
            f"""
            WITH extracted AS (
                SELECT
                    lower(reqHost) AS reqHost,
                    {candidate_expr} AS candidate_id,
                    reqPath,
                    queryStr,
                    cliIP,
                    {channel_qs} AS channel_qs,
                    {channel_name_qs} AS channel_name_qs,
                    regexp_extract(reqPath, '(vglive-sk-[0-9]+)', 1) AS path_vglive_id,
                    {platform_qs} AS platform,
                    {device_qs} AS device_name,
                    {session_qs} AS session_id,
                    {device_id_qs} AS device_id
                FROM read_parquet('{glob}', hive_partitioning=1)
                WHERE statusCode = '200'
                  {path_filter}
                  AND queryStr IS NOT NULL
                  AND queryStr <> ''
                  AND ({date_where})
                  AND ({partition_filter})
            ),
            enriched AS (
                SELECT
                    *,
                    COALESCE(
                        NULLIF(channel_qs, ''),
                        NULLIF(channel_name_qs, ''),
                        NULLIF(path_vglive_id, ''),
                        'Unknown'
                    ) AS raw_channel,
                    CASE
                        WHEN channel_qs <> '' THEN 'query_channel'
                        WHEN channel_name_qs <> '' THEN 'query_channel_name'
                        WHEN path_vglive_id <> '' THEN 'path_vglive_id'
                        ELSE 'unknown'
                    END AS channel_source
                FROM extracted
            )
            SELECT
                reqHost,
                candidate_id,
                channel_source,
                raw_channel,
                platform,
                device_name,
                COUNT(*) AS requests,
                COUNT(DISTINCT NULLIF(session_id, '')) AS sessions,
                COUNT(DISTINCT NULLIF(device_id, '')) AS devices,
                COUNT(DISTINCT cliIP) AS unique_viewers,
                any_value(reqPath) AS sample_reqPath,
                any_value(queryStr) AS sample_queryStr
            FROM enriched
            WHERE channel_source <> 'unknown'
            GROUP BY 1, 2, 3, 4, 5, 6
            ORDER BY requests DESC
            {limit_clause}
            """,
            params,
        ).fetchdf()
    finally:
        con.close()

    if df.empty:
        return df

    for column in ["raw_channel", "platform", "device_name"]:
        df[column] = df[column].fillna("").astype(str).map(unquote_plus)

    df["pure_channel"] = df.apply(
        lambda row: normalize_channel_name_smart(
            row.get("raw_channel", ""),
            row.get("platform", ""),
            row.get("device_name", ""),
        ),
        axis=1,
    )
    df["mapped_channel"] = df.apply(
        lambda row: resolve_channel(row.get("reqHost", ""), row.get("candidate_id", "")),
        axis=1,
    )

    pure_key = df["pure_channel"].fillna("").astype(str).map(lambda value: re.sub(r"[^a-z0-9]+", "", value))
    mapped_key = df["mapped_channel"].map(_compact_channel_name)
    alias_key = df["pure_channel"].map(QUERY_CHANNEL_ALIASES).fillna("").map(_compact_channel_name)
    alias_match = (alias_key != "") & (alias_key == mapped_key)
    df["review_status"] = "ok"
    df.loc[df["mapped_channel"] == "Other", "review_status"] = "unmapped_candidate"
    mismatch = (
        (df["mapped_channel"] != "Other")
        & (df["channel_source"] != "path_vglive_id")
        & (pure_key != mapped_key)
        & ~alias_match
    )
    df.loc[mismatch, "review_status"] = "query_mapping_mismatch"

    ordered_columns = [
        "review_status",
        "pure_channel",
        "raw_channel",
        "mapped_channel",
        "reqHost",
        "candidate_id",
        "channel_source",
        "platform",
        "device_name",
        "requests",
        "sessions",
        "devices",
        "unique_viewers",
        "sample_reqPath",
        "sample_queryStr",
    ]
    status_order = {"unmapped_candidate": 0, "query_mapping_mismatch": 1, "ok": 2}
    df["_status_order"] = df["review_status"].map(status_order).fillna(9)
    return (
        df[ordered_columns + ["_status_order"]]
        .sort_values(["_status_order", "requests"], ascending=[True, False])
        .drop(columns=["_status_order"])
        .reset_index(drop=True)
    )


def find_unmapped_channels(lake_path: str | Path, start_date=None, end_date=None) -> pd.DataFrame:
    glob = parquet_glob(lake_path)
    con = duckdb.connect()
    con.execute("PRAGMA threads=8")
    try:
        _create_resolved_table(con, glob, start_date, end_date)
        df = con.execute("""
            SELECT
                reqHost,
                candidate_id,
                COUNT(*) AS raw_chunks,
                COUNT(DISTINCT coalesce(cliIP, '') || '|' || reqHost || '|' || candidate_id || '|' || coalesce(reqPath, '')) AS dedup_chunks,
                COUNT(DISTINCT cliIP) AS unique_viewers,
                any_value(reqPath) AS sample_reqPath
            FROM resolved_tmp
            WHERE channel_name = 'Other'
            GROUP BY reqHost, candidate_id
            ORDER BY dedup_chunks DESC
        """).fetchdf()
        if not df.empty:
            df["watch_hours"] = df["dedup_chunks"] * CHUNK_DURATION_HOURS
        return df
    finally:
        con.close()


def export_unmapped_rows(
    lake_path: str | Path,
    output_csv: str | Path,
    start_date=None,
    end_date=None,
    max_rows: int = 10000,
) -> int:
    glob = parquet_glob(lake_path)
    output_csv = Path(output_csv)
    con = duckdb.connect()
    con.execute("PRAGMA threads=8")
    try:
        _create_resolved_table(con, glob, start_date, end_date)
        limit_clause = "" if max_rows <= 0 else f"LIMIT {int(max_rows)}"
        con.execute(f"""
            COPY (
                SELECT
                    reqHost,
                    candidate_id,
                    cliIP,
                    reqTimeSec,
                    reqPath,
                    channel_name
                FROM resolved_tmp
                WHERE channel_name = 'Other'
                ORDER BY reqHost, candidate_id
                {limit_clause}
            )
            TO '{_sql_path(output_csv)}' (HEADER, DELIMITER ',')
        """)
        return output_csv.stat().st_size if output_csv.exists() else 0
    finally:
        con.close()
