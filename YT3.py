r"""
Hybrid YouTube tracker: yt-dlp channel discovery + YouTube Data API v3 data.

YT3 uses yt-dlp to scan a channel's Streams page, so it consumes zero
search.list calls. It then batches up to 50 live video IDs into each official
videos.list request for status and concurrent viewer counts. Storage, rolling
Parquet files, multi-source support, and CLI arguments are inherited from YT2.

Setup:
    pip install -U yt-dlp pyarrow
    Put YOUTUBE_API_KEY in the .env file beside this script.

Run:
    .\venv\Scripts\python.exe YT3.py https://www.youtube.com/@indiatv 60
"""

import os
import re
from typing import List, Optional, Tuple
from urllib.parse import urlparse, urlunparse

import yt_dlp

import YT2


CHANNEL_SUFFIXES = ("live", "streams", "videos", "shorts", "featured")
FLAT_ENDED_STATUSES = {"was_live", "post_live", "not_live"}


def normalize_channel_url(source: str) -> str:
    raw = source.strip()
    if not raw:
        raise YT2.YouTubeApiError("YouTube channel cannot be empty")
    if re.fullmatch(r"UC[A-Za-z0-9_-]{20,30}", raw):
        raw = f"https://www.youtube.com/channel/{raw}"
    elif raw.startswith("@"):
        raw = f"https://www.youtube.com/{raw}"
    elif "://" not in raw:
        if raw.startswith(("youtube.com/", "www.youtube.com/")):
            raw = "https://" + raw
        else:
            raw = f"https://www.youtube.com/@{raw.lstrip('@')}"

    parsed = urlparse(raw)
    host = parsed.netloc.lower().split(":", 1)[0]
    if not host.endswith("youtube.com"):
        raise YT2.YouTubeApiError(f"Not a YouTube channel URL: {source}")
    return urlunparse(parsed._replace(query="", fragment="")).rstrip("/")


def streams_tab_url(channel_url: str) -> str:
    base = channel_url.rstrip("/")
    for suffix in CHANNEL_SUFFIXES:
        marker = "/" + suffix
        if base.lower().endswith(marker):
            base = base[: -len(marker)]
            break
    return base + "/streams"


class HybridYouTubeApi(YT2.YouTubeApiV3):
    """Discover with yt-dlp; retrieve video measurements with API v3."""

    discovery_api_calls = 0
    discovery_search_queries = 0
    discovery_notice = (
        "Hybrid discovery: yt-dlp scans the channel page every "
        "{discovery_every} poll(s); search.list quota usage is zero."
    )

    def resolve_channel(self, source: str) -> Tuple[str, Optional[str]]:
        channel_url = normalize_channel_url(source)
        return channel_url, YT2.source_name_from_url(channel_url)

    def list_live_videos(self, channel_url: str, limit: int) -> List[YT2.LiveCandidate]:
        opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "extract_flat": True,
            "playlistend": limit,
        }
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(streams_tab_url(channel_url), download=False)
        except Exception as exc:
            raise YT2.YouTubeApiError(
                f"yt-dlp channel discovery failed: {exc}"
            ) from exc

        candidates: List[YT2.LiveCandidate] = []
        seen = set()
        for entry in info.get("entries") or []:
            if not entry or not entry.get("id"):
                continue
            video_id = str(entry["id"])
            if video_id in seen:
                continue
            status = entry.get("live_status")
            if status in FLAT_ENDED_STATUSES or status == "is_upcoming":
                continue
            if status is None and entry.get("duration") is not None:
                continue
            seen.add(video_id)
            candidates.append(YT2.LiveCandidate(video_id, entry.get("title")))
        return candidates


def build_parser():
    parser = YT2.build_parser()
    parser.description = __doc__.splitlines()[1]
    parser.set_defaults(discovery_every=1)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    dotenv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    api_key = (
        args.api_key
        or os.environ.get("YOUTUBE_API_KEY")
        or os.environ.get("YOUTUBE_DATA_API_KEY")
        or YT2.load_dotenv_value(
            dotenv_path, "YOUTUBE_API_KEY", "YOUTUBE_DATA_API_KEY"
        )
    )
    if not api_key or not api_key.strip():
        parser.error("YouTube API key required in .env or --api-key")

    urls = [args.url, *args.extra_url]
    try:
        for url_file in args.url_file:
            urls.extend(YT2.load_url_file(url_file))
    except OSError as exc:
        parser.error(f"could not read URL file: {exc}")

    try:
        YT2.track_many(
            urls,
            api_key,
            args.interval_sec,
            args.out_dir,
            args.scan_limit,
            args.workers,
            args.discovery_every,
            args.end_confirmations,
            args.row_group_rows,
            args.roll_minutes,
            args.once,
            args.request_timeout,
            api_class=HybridYouTubeApi,
        )
    except (ValueError, YT2.YouTubeApiError, OSError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
