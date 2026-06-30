"""
YouTube Live Viewer Count Tracker (No API Key Needed) -- Parquet Edition
--------------------------------------------------------------------------
Uses yt-dlp, which parses publicly visible YouTube page data, to read
live concurrent viewer counts without needing a Google Cloud project
or API key. Stores results as Parquet instead of CSV.

Accepts either:
    - A direct video URL, e.g. https://www.youtube.com/watch?v=VIDEO_ID
      -> tracks that single video.
    - A channel URL, e.g. https://www.youtube.com/@indiatv
      -> on every poll, first discovers every stream that is CURRENTLY
         live on the channel (big news channels often run several at
         once), then logs each one's viewer count -- discovery and
         tracking happen together as one automated cycle, every
         interval, minute-on-minute.

Storage:
    Each run creates ONE new Parquet file inside an output folder,
    named by the run's start time, e.g.:
        viewer_logs/log_2026-06-30_115000.parquet
    Restarting the script never overwrites previous runs -- you get a
    small Parquet "lake" you can query all at once later, e.g. with
    DuckDB:
        SELECT * FROM read_parquet('viewer_logs/*.parquet')
        ORDER BY date, time;

Note: this relies on YouTube's page structure rather than an official,
versioned API. It can break if YouTube changes their site. Channel
mode makes one request per candidate stream on every poll, so for
channels with many simultaneous live streams, use a longer interval
(90-120s+) to avoid getting rate-limited.

Setup:
    pip install -U yt-dlp pyarrow

Usage:
    python yt.py URL [interval_sec] [out_dir] [scan_limit]

Examples:
    python yt.py https://www.youtube.com/watch?v=VIDEO_ID 60
    python yt.py https://www.youtube.com/@indiatv 90
"""

import os
import sys
import time
import datetime
import yt_dlp
from yt_dlp.utils import DownloadError
import pyarrow as pa
import pyarrow.parquet as pq

# Avoid crashes when printing non-English characters (e.g. Hindi titles) on Windows terminals
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

YDL_OPTS = {
    "quiet": True,
    "no_warnings": True,
    "skip_download": True,
}

CHANNEL_MARKERS = ("/@", "/channel/", "/c/", "/user/")

SCHEMA = pa.schema([
    ("date", pa.string()),
    ("time", pa.string()),
    ("video_id", pa.string()),
    ("title", pa.string()),
    ("concurrent_viewers", pa.int64()),
    ("status", pa.string()),
])


def is_channel_url(url: str) -> bool:
    return any(m in url for m in CHANNEL_MARKERS) and "watch?v=" not in url


def get_video_status(video_url: str):
    """Full lookup for one video. Returns (video_id, title, viewers, status)."""
    try:
        with yt_dlp.YoutubeDL(YDL_OPTS) as ydl:
            info = ydl.extract_info(video_url, download=False)
    except DownloadError:
        return None, None, None, "not_found_or_not_live"

    return (
        info.get("id"),
        info.get("title"),
        info.get("concurrent_view_count"),
        info.get("live_status"),  # is_live, was_live, is_upcoming, not_live
    )


def get_channel_streams_tab_url(channel_url: str) -> str:
    base = channel_url.rstrip("/")
    for suffix in ("/live", "/streams", "/videos", "/shorts", "/featured"):
        if base.endswith(suffix):
            base = base[: -len(suffix)]
            break
    return base + "/streams"


def list_candidate_video_ids(channel_url: str, limit: int):
    """Cheaply lists recent entries from the channel's Live/Streams tab
    (just ids, no per-video data yet -- keeps scanning fast)."""
    streams_url = get_channel_streams_tab_url(channel_url)
    opts = dict(YDL_OPTS)
    opts["extract_flat"] = True
    opts["playlistend"] = limit
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(streams_url, download=False)
    entries = info.get("entries") or []
    return [e["id"] for e in entries if e and e.get("id")]


def get_all_live_videos(channel_url: str, limit: int):
    """Returns a list of (video_id, title, viewers) for every video on the
    channel's Live tab that is CURRENTLY live (checked individually,
    since the flat tab listing doesn't include live status)."""
    candidate_ids = list_candidate_video_ids(channel_url, limit)
    live = []
    for vid in candidate_ids:
        video_url = f"https://www.youtube.com/watch?v={vid}"
        _, title, viewers, status = get_video_status(video_url)
        if status == "is_live":
            live.append((vid, title, viewers))
    return live


def sleep_until_next_boundary(interval_sec: int):
    """Sleeps just long enough to land on the next clean interval boundary
    (e.g. the next :00 second mark for a 60s interval), so readings line
    up minute-on-minute instead of drifting based on script start time."""
    now = time.time()
    sleep_time = interval_sec - (now % interval_sec)
    time.sleep(sleep_time)


def write_batch(writer: pq.ParquetWriter, rows: list):
    """rows: list of tuples in SCHEMA column order"""
    if not rows:
        return
    columns = list(zip(*rows))
    arrays = [pa.array(col) for col in columns]
    table = pa.Table.from_arrays(arrays, schema=SCHEMA)
    writer.write_table(table)


def track(input_url: str, interval_sec: int = 60, out_dir: str = "viewer_logs", scan_limit: int = 10):
    channel_mode = is_channel_url(input_url)
    label = f"all live streams on {input_url}" if channel_mode else input_url

    os.makedirs(out_dir, exist_ok=True)
    run_ts = datetime.datetime.now(IST).strftime("%Y-%m-%d_%H%M%S")
    out_path = os.path.join(out_dir, f"log_{run_ts}.parquet")

    print(f"Tracking {label} every {interval_sec}s. Press Ctrl+C to stop.")
    print(f"Writing to: {out_path}")

    writer = pq.ParquetWriter(out_path, SCHEMA)
    known_titles = {}      # video_id -> title, remembered even after a stream ends
    previously_live = set()  # video_ids that were live as of the last poll

    try:
        while True:
            now = datetime.datetime.now(IST)
            date_str = now.strftime("%d/%m/%y")
            time_str = now.strftime("%H:%M:%S")
            rows = []

            if channel_mode:
                try:
                    live_videos = get_all_live_videos(input_url, scan_limit)
                except Exception as e:
                    live_videos = []
                    print(f"{date_str} {time_str} IST  Error scanning channel: {e}")

                currently_live = set()

                if not live_videos:
                    print(f"{date_str} {time_str} IST  No live streams found right now.")
                    rows.append((date_str, time_str, None, None, None, "no_live_streams"))
                else:
                    print(f"{date_str} {time_str} IST  {len(live_videos)} live stream(s) found:")
                    for vid, title, viewers in live_videos:
                        currently_live.add(vid)
                        known_titles[vid] = title
                        rows.append((date_str, time_str, vid, title, viewers, "is_live"))
                        print(f'    [{vid}] viewers={viewers}  "{title}"')

                newly_started = currently_live - previously_live
                newly_ended = previously_live - currently_live

                for vid in newly_started:
                    print(f'    >> NEW STREAM DETECTED: [{vid}] "{known_titles.get(vid)}"')

                for vid in newly_ended:
                    print(f'    >> STREAM ENDED: [{vid}] "{known_titles.get(vid)}"')
                    rows.append((date_str, time_str, vid, known_titles.get(vid), None, "stream_ended"))

                previously_live = currently_live
                write_batch(writer, rows)

            else:
                try:
                    vid, title, viewers, status = get_video_status(input_url)
                except Exception as e:
                    vid, title, viewers, status = None, None, None, f"error: {e}"

                rows.append((date_str, time_str, vid, title, viewers, status))
                write_batch(writer, rows)
                print(f"{date_str} {time_str} IST  viewers={viewers}  status={status}")

                if status == "was_live":
                    print("Stream has ended. Stopping.")
                    break

            sleep_until_next_boundary(interval_sec)
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        writer.close()
        print(f"Parquet file saved: {out_path}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python yt.py URL [interval_sec] [out_dir] [scan_limit]")
        sys.exit(1)

    url = sys.argv[1]
    interval = int(sys.argv[2]) if len(sys.argv) > 2 else 60
    outdir = sys.argv[3] if len(sys.argv) > 3 else "viewer_logs"
    scanlimit = int(sys.argv[4]) if len(sys.argv) > 4 else 10

    track(url, interval, outdir, scanlimit)