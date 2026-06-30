r"""
YouTube Live Viewer Count Tracker (YouTube Data API v3) -- Parquet Edition
----------------------------------------------------------------------------
Tracks one or more YouTube videos/channels with the official YouTube Data API
v3. It discovers channel livestreams with search.list and reads live status and
concurrent viewers with videos.list. Unlike YT.py, this file does not use
yt-dlp or scrape YouTube pages.

Requirements:
    pip install pyarrow

API key:
    Put YOUTUBE_API_KEY in a .env file beside this script, set it as an
    environment variable, or pass --api-key. The key needs access to YouTube
    Data API v3 in its Google Cloud project.

Usage (the original positional arguments remain compatible):
    python YT2.py URL [interval_sec] [out_dir] [scan_limit] [options]

Examples:
    $env:YOUTUBE_API_KEY = "your-key"
    python YT2.py https://www.youtube.com/watch?v=VIDEO_ID 60
    python YT2.py https://www.youtube.com/@indiatv 90
    python YT2.py https://www.youtube.com/@indiatv 90 --once
    python YT2.py https://www.youtube.com/@indiatv 90 --extra-url https://www.youtube.com/@aajtak
    python YT2.py https://www.youtube.com/@indiatv 90 --url-file channels.txt

Useful options:
    --api-key KEY          API key (prefer the YOUTUBE_API_KEY environment variable).
    --extra-url URL        Add another channel/video (repeatable).
    --url-file PATH        Add URLs from a text file, one per line (repeatable).
    --discovery-every N    Run live search every N polls (default: 15).
    --workers N            Maximum concurrent API requests/sources (default: 4).
    --end-confirmations N  Successful non-live checks required (default: 2).
    --row-group-rows N     Buffered rows per Parquet row group (default: 250).
    --roll-minutes N       Close on aligned IST N-minute boundaries (default: 60).
    --request-timeout N    HTTP timeout in seconds (default: 20).
    --once                 Run one poll, print its timing, and exit.

Quota note:
    A channel discovery consumes one search.list call from the Search Queries
    quota bucket (normally 100 calls/day). Each batched videos.list lookup uses
    1 standard quota unit. Increase --discovery-every when polling frequently
    or tracking many channels.

Storage:
    Files use the same schema and Hive-style output layout as YT.py:
      <out_dir>\source=Youtube\year=YYYY\month=MM\day=DD\*.parquet
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
import re
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import Request, urlopen

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ModuleNotFoundError:
    pa = None
    pq = None


try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30), name="IST")
DEFAULT_OUTPUT_DIR = r"Y:\Veto Logs Backup\DO NOT DELETE"
YOUTUBE_SOURCE_PARTITION = "source=Youtube"
API_BASE_URL = "https://www.googleapis.com/youtube/v3"
CHANNEL_SUFFIXES = ("live", "streams", "videos", "shorts", "featured")
ENDED_STATUSES = {"was_live", "not_live", "not_found"}
DIRECT_STOP_STATUSES = {"was_live"}
PRINT_LOCK = threading.Lock()

SCHEMA = None if pa is None else pa.schema([
    ("date", pa.string()),
    ("time", pa.string()),
    ("video_id", pa.string()),
    ("title", pa.string()),
    ("concurrent_viewers", pa.int64()),
    ("status", pa.string()),
])

Row = Tuple[str, str, Optional[str], Optional[str], Optional[int], str]


class YouTubeApiError(RuntimeError):
    """A sanitized YouTube API error that never includes the API key."""

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        reason: Optional[str] = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.reason = reason


@dataclass(frozen=True)
class VideoLookup:
    video_id: str
    title: Optional[str]
    viewers: Optional[int]
    status: str
    error: Optional[str] = None


@dataclass(frozen=True)
class LiveCandidate:
    video_id: str
    title: Optional[str]


@dataclass(frozen=True)
class ResolvedSource:
    input_url: str
    kind: str
    source_id: str
    title: Optional[str]


@dataclass
class ChannelState:
    active_ids: Set[str] = field(default_factory=set)
    known_titles: Dict[str, Optional[str]] = field(default_factory=dict)
    end_misses: Dict[str, int] = field(default_factory=dict)
    poll_number: int = 0


@dataclass(frozen=True)
class PollOutcome:
    rows: List[Row]
    api_calls: int
    search_queries: int
    quota_units: int
    should_stop: bool
    elapsed: float


def emit(message: str, prefix: str = "", error: bool = False) -> None:
    rendered = f"[{prefix}] {message}" if prefix else message
    with PRINT_LOCK:
        print(rendered, file=sys.stderr if error else sys.stdout)


def _as_int(value) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class YouTubeApiV3:
    discovery_api_calls = 1
    discovery_search_queries = 1
    discovery_notice = (
        "Quota reminder: each due channel discovery uses one search query; "
        "--discovery-every is {discovery_every}."
    )

    def __init__(
        self,
        api_key: str,
        timeout: float = 20.0,
        request_gate: Optional[threading.BoundedSemaphore] = None,
    ):
        if not api_key.strip():
            raise ValueError("YouTube API key cannot be empty")
        self.api_key = api_key.strip()
        self.timeout = timeout
        self.request_gate = request_gate

    def _request(self, resource: str, params: Dict[str, object]) -> Dict[str, object]:
        query = dict(params)
        query["key"] = self.api_key
        url = f"{API_BASE_URL}/{resource}?{urlencode(query)}"
        request = Request(url, headers={"Accept": "application/json"})

        try:
            if self.request_gate is None:
                with urlopen(request, timeout=self.timeout) as response:
                    return json.loads(response.read().decode("utf-8"))
            self.request_gate.acquire()
            try:
                with urlopen(request, timeout=self.timeout) as response:
                    return json.loads(response.read().decode("utf-8"))
            finally:
                self.request_gate.release()
        except HTTPError as exc:
            try:
                payload = json.loads(exc.read().decode("utf-8", errors="replace"))
                error_payload = payload.get("error") or {}
                details = error_payload.get("errors") or []
                reason = details[0].get("reason") if details else None
                message = error_payload.get("message") or str(exc.reason)
            except Exception:
                reason = None
                message = str(exc.reason)
            raise YouTubeApiError(
                f"YouTube API HTTP {exc.code}: {message}",
                exc.code,
                reason,
            ) from exc
        except URLError as exc:
            raise YouTubeApiError(f"YouTube API connection error: {exc.reason}") from exc
        except TimeoutError as exc:
            raise YouTubeApiError("YouTube API request timed out") from exc
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise YouTubeApiError("YouTube API returned an invalid JSON response") from exc

    def _channel_lookup(self, **identifier: str) -> Tuple[str, Optional[str]]:
        payload = self._request(
            "channels",
            {
                "part": "id,snippet",
                "maxResults": 1,
                "fields": "items(id,snippet(title))",
                **identifier,
            },
        )
        items = payload.get("items") or []
        if not items:
            label = next(iter(identifier.values()))
            raise YouTubeApiError(f"YouTube channel not found: {label}")
        item = items[0]
        return str(item["id"]), (item.get("snippet") or {}).get("title")

    def resolve_channel(self, source: str) -> Tuple[str, Optional[str]]:
        """Resolve channel IDs, @handles, /channel/, /user/, and legacy URLs."""

        raw = source.strip()
        if re.fullmatch(r"UC[A-Za-z0-9_-]{20,30}", raw):
            return self._channel_lookup(id=raw)
        if raw.startswith("@") and "/" not in raw:
            return self._channel_lookup(forHandle=raw[1:])

        parsed = urlparse(raw if "://" in raw else "https://www.youtube.com/" + raw)
        parts = [part for part in parsed.path.split("/") if part]
        while parts and parts[-1].lower() in CHANNEL_SUFFIXES:
            parts.pop()
        if not parts:
            raise YouTubeApiError(f"Could not identify a YouTube channel in: {source}")

        first = parts[0]
        if first == "channel" and len(parts) >= 2:
            return self._channel_lookup(id=parts[1])
        if first == "user" and len(parts) >= 2:
            return self._channel_lookup(forUsername=parts[1])
        if first.startswith("@"):
            return self._channel_lookup(forHandle=first[1:])

        # The API has no exact legacy /c/custom-name resolver. Search is the
        # official v3 fallback; callers are told which result was selected.
        query = parts[1] if first == "c" and len(parts) >= 2 else first
        payload = self._request(
            "search",
            {
                "part": "snippet",
                "q": query,
                "type": "channel",
                "maxResults": 1,
                "fields": "items(id(channelId),snippet(channelTitle,title))",
            },
        )
        items = payload.get("items") or []
        if not items:
            raise YouTubeApiError(f"YouTube channel not found: {query}")
        item = items[0]
        channel_id = (item.get("id") or {}).get("channelId")
        snippet = item.get("snippet") or {}
        if not channel_id:
            raise YouTubeApiError(f"YouTube channel result had no channel ID: {query}")
        return str(channel_id), snippet.get("channelTitle") or snippet.get("title")

    def list_live_videos(self, channel_id: str, limit: int) -> List[LiveCandidate]:
        payload = self._request(
            "search",
            {
                "part": "snippet",
                "channelId": channel_id,
                "eventType": "live",
                "type": "video",
                "order": "date",
                "maxResults": min(limit, 50),
                "fields": "items(id(videoId),snippet(title))",
            },
        )
        candidates: List[LiveCandidate] = []
        for item in payload.get("items") or []:
            video_id = (item.get("id") or {}).get("videoId")
            if video_id:
                candidates.append(
                    LiveCandidate(str(video_id), (item.get("snippet") or {}).get("title"))
                )
        return candidates

    def get_videos(self, video_ids: Iterable[str]) -> Dict[str, VideoLookup]:
        unique_ids = list(dict.fromkeys(str(value) for value in video_ids if value))
        if not unique_ids:
            return {}

        results: Dict[str, VideoLookup] = {}
        for start in range(0, len(unique_ids), 50):
            chunk = unique_ids[start : start + 50]
            try:
                payload = self._request(
                    "videos",
                    {
                        "part": "id,snippet,liveStreamingDetails",
                        "id": ",".join(chunk),
                        "maxResults": 50,
                        "fields": (
                            "items(id,snippet(title,liveBroadcastContent),"
                            "liveStreamingDetails(actualStartTime,actualEndTime,"
                            "scheduledStartTime,concurrentViewers))"
                        ),
                    },
                )
            except YouTubeApiError as exc:
                for video_id in chunk:
                    results[video_id] = VideoLookup(
                        video_id, None, None, "lookup_error", str(exc)
                    )
                continue

            returned_ids: Set[str] = set()
            for item in payload.get("items") or []:
                video_id = str(item["id"])
                returned_ids.add(video_id)
                snippet = item.get("snippet") or {}
                details = item.get("liveStreamingDetails") or {}
                broadcast_content = snippet.get("liveBroadcastContent")

                if details.get("actualEndTime"):
                    status = "was_live"
                elif details.get("actualStartTime") or broadcast_content == "live":
                    status = "is_live"
                elif details.get("scheduledStartTime") or broadcast_content == "upcoming":
                    status = "is_upcoming"
                else:
                    status = "not_live"

                results[video_id] = VideoLookup(
                    video_id,
                    snippet.get("title"),
                    _as_int(details.get("concurrentViewers")),
                    status,
                )

            for video_id in chunk:
                if video_id not in returned_ids:
                    results[video_id] = VideoLookup(
                        video_id,
                        None,
                        None,
                        "not_found",
                        "video is private, deleted, or unavailable to the API key",
                    )
        return results


def video_id_from_source(source: str) -> Optional[str]:
    raw = source.strip()
    if re.fullmatch(r"[A-Za-z0-9_-]{11}", raw) and not raw.startswith("UC"):
        return raw

    parsed = urlparse(raw if "://" in raw else "https://" + raw)
    host = parsed.netloc.lower().split(":", 1)[0]
    parts = [part for part in parsed.path.split("/") if part]
    candidate: Optional[str] = None

    if host in {"youtu.be", "www.youtu.be"} and parts:
        candidate = parts[0]
    elif host.endswith("youtube.com"):
        if parsed.path.rstrip("/") == "/watch":
            candidate = (parse_qs(parsed.query).get("v") or [None])[0]
        elif len(parts) >= 2 and parts[0] in {"embed", "shorts"}:
            candidate = parts[1]
        elif len(parts) >= 2 and parts[0] == "live" and not parts[1].startswith("@"):
            candidate = parts[1]

    if candidate and re.fullmatch(r"[A-Za-z0-9_-]{6,20}", candidate):
        return candidate
    return None


def resolve_source(api: YouTubeApiV3, source: str) -> ResolvedSource:
    video_id = video_id_from_source(source)
    if video_id:
        lookup = api.get_videos([video_id]).get(video_id)
        if lookup and lookup.status == "lookup_error":
            raise YouTubeApiError(lookup.error or f"Could not resolve video: {video_id}")
        if not lookup or lookup.status == "not_found":
            raise YouTubeApiError(f"YouTube video not found or unavailable: {video_id}")
        title = lookup.title if lookup else None
        return ResolvedSource(source, "video", video_id, title)

    channel_id, title = api.resolve_channel(source)
    return ResolvedSource(source, "channel", channel_id, title)


def sleep_until_next_boundary(interval_sec: int) -> None:
    now = time.time()
    time.sleep(interval_sec - (now % interval_sec))


def write_batch(writer: pq.ParquetWriter, rows: Sequence[Row]) -> None:
    if not rows:
        return
    columns = list(zip(*rows))
    arrays = [
        pa.array(column, type=field.type)
        for column, field in zip(columns, SCHEMA)
    ]
    writer.write_table(pa.Table.from_arrays(arrays, schema=SCHEMA))


class RollingParquetLogger:
    """Publish clock-aligned files in IST year/month/day Hive partitions."""

    def __init__(
        self,
        out_dir: str,
        row_group_rows: int,
        roll_minutes: int,
        file_prefix: str = "Youtube",
        now_provider=None,
    ):
        self.out_dir = out_dir
        self.row_group_rows = row_group_rows
        self.roll_seconds = roll_minutes * 60
        self.file_prefix = re.sub(
            r'[<>:"/\\|?*\x00-\x1f]+', "_", file_prefix
        ).strip(" ._") or "Youtube"
        self.now_provider = now_provider or (lambda: datetime.datetime.now(IST))
        self.buffer: List[Row] = []
        self.writer: Optional[pq.ParquetWriter] = None
        self.partial_path: Optional[str] = None
        self.final_path: Optional[str] = None
        self.segment_start: Optional[datetime.datetime] = None
        self.segment_end: Optional[datetime.datetime] = None
        os.makedirs(out_dir, exist_ok=True)
        self._open_segment()

    def _segment_bounds(
        self, now_ist: datetime.datetime
    ) -> Tuple[datetime.datetime, datetime.datetime]:
        midnight = now_ist.replace(hour=0, minute=0, second=0, microsecond=0)
        seconds_from_midnight = (now_ist - midnight).total_seconds()
        bucket = int(seconds_from_midnight // self.roll_seconds)
        start = midnight + datetime.timedelta(seconds=bucket * self.roll_seconds)
        return start, start + datetime.timedelta(seconds=self.roll_seconds)

    def _open_segment(self, reference_time: Optional[datetime.datetime] = None) -> None:
        now_ist = (reference_time or self.now_provider()).astimezone(IST)
        self.segment_start, self.segment_end = self._segment_bounds(now_ist)
        partition_dir = os.path.join(
            self.out_dir,
            f"year={self.segment_start:%Y}",
            f"month={self.segment_start:%m}",
            f"day={self.segment_start:%d}",
        )
        os.makedirs(partition_dir, exist_ok=True)
        stamp = self.segment_start.strftime("%d-%m-%Y_%H-%M")
        token = uuid.uuid4().hex[:8]
        filename = f"{self.file_prefix}_{stamp}_{token}.parquet"
        self.final_path = os.path.join(partition_dir, filename)
        self.partial_path = self.final_path + ".partial"
        self.writer = pq.ParquetWriter(self.partial_path, SCHEMA)

    def _flush_rows(self, count: int) -> None:
        if not self.writer or count <= 0:
            return
        batch = self.buffer[:count]
        write_batch(self.writer, batch)
        del self.buffer[:count]

    def _flush_complete_groups(self) -> None:
        while len(self.buffer) >= self.row_group_rows:
            self._flush_rows(self.row_group_rows)

    def _close_segment(self) -> Optional[str]:
        if not self.writer:
            return None
        self._flush_rows(len(self.buffer))
        self.writer.close()
        self.writer = None
        os.replace(self.partial_path, self.final_path)
        return self.final_path

    def add_rows(self, rows: Sequence[Row]) -> Optional[str]:
        completed_path = None
        row_time = self.now_provider().astimezone(IST)
        if rows:
            try:
                row_time = datetime.datetime.strptime(
                    f"{rows[0][0]} {rows[0][1]}", "%Y-%m-%d %H:%M:%S"
                ).replace(tzinfo=IST)
            except (TypeError, ValueError):
                pass
        outside_segment = (
            self.segment_start is not None
            and self.segment_end is not None
            and not (self.segment_start <= row_time < self.segment_end)
        )
        if outside_segment:
            completed_path = self._close_segment()
            self._open_segment(row_time)
        self.buffer.extend(rows)
        self._flush_complete_groups()
        return completed_path

    def close(self) -> Optional[str]:
        return self._close_segment()


def make_row(
    now_ist: datetime.datetime,
    video_id: Optional[str],
    title: Optional[str],
    viewers: Optional[int],
    status: str,
) -> Row:
    return (
        now_ist.strftime("%Y-%m-%d"),
        now_ist.strftime("%H:%M:%S"),
        video_id,
        title,
        viewers,
        status,
    )


def poll_channel(
    api: YouTubeApiV3,
    channel_id: str,
    now_ist: datetime.datetime,
    state: ChannelState,
    scan_limit: int,
    discovery_every: int,
    end_confirmations: int,
    log_prefix: str = "",
) -> Tuple[List[Row], int, int, int]:
    state.poll_number += 1
    discovery_due = (state.poll_number - 1) % discovery_every == 0
    api_calls = 0
    search_queries = 0
    quota_units = 0
    discovery_error: Optional[str] = None
    candidates: List[LiveCandidate] = []

    if discovery_due:
        api_calls += getattr(api, "discovery_api_calls", 1)
        search_queries += getattr(api, "discovery_search_queries", 1)
        try:
            candidates = api.list_live_videos(channel_id, scan_limit)
        except YouTubeApiError as exc:
            discovery_error = str(exc)
            emit(
                f"{now_ist:%Y-%m-%d %H:%M:%S} IST  Channel discovery error: {exc}",
                log_prefix,
                error=True,
            )

    for candidate in candidates:
        if candidate.title:
            state.known_titles[candidate.video_id] = candidate.title

    lookup_ids = list(dict.fromkeys([
        *sorted(state.active_ids),
        *(candidate.video_id for candidate in candidates),
    ]))
    if lookup_ids:
        api_calls += (len(lookup_ids) + 49) // 50
        quota_units += (len(lookup_ids) + 49) // 50
    results = api.get_videos(lookup_ids)

    previous_active = set(state.active_ids)
    next_active: Set[str] = set()
    confirmed_live: Set[str] = set()
    rows: List[Row] = []

    for video_id in lookup_ids:
        result = results.get(
            video_id,
            VideoLookup(video_id, None, None, "lookup_error", "missing API result"),
        )
        title = result.title or state.known_titles.get(video_id)
        if title:
            state.known_titles[video_id] = title

        if result.status == "is_live":
            next_active.add(video_id)
            confirmed_live.add(video_id)
            state.end_misses.pop(video_id, None)
            rows.append(make_row(now_ist, video_id, title, result.viewers, "is_live"))
            continue

        if video_id in previous_active:
            if result.status == "lookup_error":
                next_active.add(video_id)
                rows.append(make_row(now_ist, video_id, title, None, "lookup_error"))
                emit(
                    f"!! [{video_id}] lookup failed; keeping prior live state: {result.error}",
                    log_prefix,
                    error=True,
                )
                continue

            misses = state.end_misses.get(video_id, 0) + 1
            state.end_misses[video_id] = misses
            if misses >= end_confirmations:
                state.end_misses.pop(video_id, None)
                rows.append(make_row(now_ist, video_id, title, None, "stream_ended"))
                emit(f'>> STREAM ENDED: [{video_id}] "{title}"', log_prefix)
            else:
                next_active.add(video_id)
                rows.append(make_row(now_ist, video_id, title, None, "end_pending"))
                emit(
                    f"?? [{video_id}] non-live confirmation "
                    f"{misses}/{end_confirmations} ({result.status})",
                    log_prefix,
                )
        elif result.status == "lookup_error":
            rows.append(make_row(now_ist, video_id, title, None, "lookup_error"))
            emit(f"!! [{video_id}] candidate lookup failed: {result.error}", log_prefix, True)

    newly_started = confirmed_live - previous_active
    for video_id in sorted(newly_started):
        emit(
            f'>> NEW STREAM DETECTED: [{video_id}] "{state.known_titles.get(video_id)}"',
            log_prefix,
        )

    state.active_ids = next_active

    if confirmed_live:
        emit(
            f"{now_ist:%Y-%m-%d %H:%M:%S} IST  "
            f"{len(confirmed_live)} confirmed live stream(s):",
            log_prefix,
        )
        for row in rows:
            if row[-1] == "is_live":
                emit(f'[{row[2]}] viewers={row[4]}  "{row[3]}"', log_prefix)

    if not rows:
        if discovery_error:
            rows.append(make_row(now_ist, None, None, None, "channel_discovery_error"))
        else:
            rows.append(make_row(now_ist, None, None, None, "no_live_streams"))
            label = "No live streams found." if discovery_due else "No known live streams."
            emit(f"{now_ist:%Y-%m-%d %H:%M:%S} IST  {label}", log_prefix)

    return rows, api_calls, search_queries, quota_units


@dataclass
class SourceRuntime:
    source: ResolvedSource
    slug: str
    logger: RollingParquetLogger
    channel_state: ChannelState = field(default_factory=ChannelState)
    stopped: bool = False


def source_name_from_url(url: str) -> str:
    parsed = urlparse(url)
    path_parts = [part.lstrip("@") for part in parsed.path.split("/") if part]
    human_part = path_parts[-1] if path_parts else parsed.netloc or url or "youtube"
    safe_part = re.sub(r"[^A-Za-z0-9._-]+", "-", human_part).strip("-._")
    return (safe_part or "youtube")[:48]


def source_slug(url: str) -> str:
    safe_part = source_name_from_url(url)
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:8]
    return f"{safe_part}-{digest}"


def youtube_output_root(out_dir: str) -> str:
    normalized = os.path.normpath(out_dir)
    if os.path.basename(normalized).lower() == YOUTUBE_SOURCE_PARTITION.lower():
        return normalized
    return os.path.join(normalized, YOUTUBE_SOURCE_PARTITION)


def poll_source(
    api: YouTubeApiV3,
    runtime: SourceRuntime,
    scan_limit: int,
    discovery_every: int,
    end_confirmations: int,
    log_prefix: str,
) -> PollOutcome:
    started = time.perf_counter()
    now_ist = datetime.datetime.now(IST)

    if runtime.source.kind == "channel":
        rows, api_calls, search_queries, quota_units = poll_channel(
            api,
            runtime.source.source_id,
            now_ist,
            runtime.channel_state,
            scan_limit,
            discovery_every,
            end_confirmations,
            log_prefix,
        )
        should_stop = False
    else:
        video_id = runtime.source.source_id
        result = api.get_videos([video_id]).get(
            video_id,
            VideoLookup(video_id, runtime.source.title, None, "lookup_error", "missing API result"),
        )
        title = result.title or runtime.source.title
        rows = [make_row(now_ist, video_id, title, result.viewers, result.status)]
        api_calls = 1
        search_queries = 0
        quota_units = 1
        should_stop = result.status in DIRECT_STOP_STATUSES
        if result.status == "lookup_error":
            emit(
                f"{now_ist:%Y-%m-%d %H:%M:%S} IST  Lookup error: {result.error}",
                log_prefix,
                error=True,
            )
        else:
            emit(
                f"{now_ist:%Y-%m-%d %H:%M:%S} IST  "
                f"viewers={result.viewers}  status={result.status}",
                log_prefix,
            )

    return PollOutcome(
        rows,
        api_calls,
        search_queries,
        quota_units,
        should_stop,
        time.perf_counter() - started,
    )


def _require_positive(name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero (got {value})")


def track_many(
    input_urls: Sequence[str],
    api_key: str,
    interval_sec: int = 60,
    out_dir: str = DEFAULT_OUTPUT_DIR,
    scan_limit: int = 10,
    max_workers: int = 4,
    discovery_every: int = 15,
    end_confirmations: int = 2,
    row_group_rows: int = 250,
    roll_minutes: int = 60,
    once: bool = False,
    request_timeout: float = 20.0,
    api_class=YouTubeApiV3,
) -> None:
    if pa is None or pq is None:
        raise ValueError("pyarrow is required; install it with: pip install pyarrow")
    for name, value in (
        ("interval_sec", interval_sec),
        ("scan_limit", scan_limit),
        ("max_workers", max_workers),
        ("discovery_every", discovery_every),
        ("end_confirmations", end_confirmations),
        ("row_group_rows", row_group_rows),
        ("roll_minutes", roll_minutes),
    ):
        _require_positive(name, value)
    if scan_limit > 50:
        raise ValueError("scan_limit cannot exceed the API maximum of 50")
    if request_timeout <= 0:
        raise ValueError("request_timeout must be greater than zero")

    urls = list(dict.fromkeys(url.strip() for url in input_urls if url.strip()))
    if not urls:
        raise ValueError("at least one YouTube URL is required")

    request_gate = threading.BoundedSemaphore(max_workers)
    api = api_class(api_key, request_timeout, request_gate)
    resolved: List[ResolvedSource] = []
    for url in urls:
        emit(f"Resolving source: {url}")
        source = resolve_source(api, url)
        resolved.append(source)
        emit(
            f'Resolved {source.kind}: {source.source_id}  "{source.title or "unknown title"}"'
        )

    multiple_sources = len(resolved) > 1
    runtimes: List[SourceRuntime] = []
    partition_root = youtube_output_root(out_dir)
    try:
        for source in resolved:
            name = source_name_from_url(source.input_url)
            runtimes.append(
                SourceRuntime(
                    source,
                    source_slug(source.input_url),
                    RollingParquetLogger(
                        partition_root,
                        row_group_rows,
                        roll_minutes,
                        name,
                    ),
                )
            )
    except Exception:
        for runtime in runtimes:
            runtime.logger.close()
        raise

    emit(
        f"Tracking {len(runtimes)} source(s) every {interval_sec}s with "
        f"up to {max_workers} concurrent API request(s). Press Ctrl+C to stop."
    )
    if any(runtime.source.kind == "channel" for runtime in runtimes):
        emit(api.discovery_notice.format(discovery_every=discovery_every))
    for runtime in runtimes:
        prefix = runtime.slug if multiple_sources else ""
        emit(f"Source: {runtime.source.input_url}", prefix)
        emit(f"Writing active segment to: {runtime.logger.partial_path}", prefix)

    source_worker_count = min(len(runtimes), max_workers)
    try:
        with ThreadPoolExecutor(
            max_workers=source_worker_count, thread_name_prefix="yt-api-source"
        ) as source_executor:
            while True:
                active = [runtime for runtime in runtimes if not runtime.stopped]
                if not active:
                    break

                cycle_started = time.perf_counter()
                future_map = {
                    source_executor.submit(
                        poll_source,
                        api,
                        runtime,
                        scan_limit,
                        discovery_every,
                        end_confirmations,
                        runtime.slug if multiple_sources else "",
                    ): runtime
                    for runtime in active
                }

                for future, runtime in future_map.items():
                    prefix = runtime.slug if multiple_sources else ""
                    try:
                        outcome = future.result()
                    except Exception as exc:
                        now_ist = datetime.datetime.now(IST)
                        emit(f"Unexpected poll error: {exc}", prefix, error=True)
                        outcome = PollOutcome(
                            [make_row(now_ist, None, None, None, "source_error")],
                            0,
                            0,
                            0,
                            False,
                            time.perf_counter() - cycle_started,
                        )

                    completed_path = runtime.logger.add_rows(outcome.rows)
                    if completed_path:
                        emit(f"Parquet segment saved: {completed_path}", prefix)
                        emit(f"Writing active segment to: {runtime.logger.partial_path}", prefix)

                    emit(
                        f"Poll benchmark: {outcome.elapsed:.3f}s, "
                        f"api_calls={outcome.api_calls}, "
                        f"search_queries={outcome.search_queries}, "
                        f"standard_quota_units={outcome.quota_units}, "
                        f"rows={len(outcome.rows)}",
                        prefix,
                    )

                    if outcome.should_stop:
                        runtime.stopped = True
                        emit("Stream has ended. Stopping this source.", prefix)

                if multiple_sources:
                    emit(
                        f"Global cycle benchmark: "
                        f"{time.perf_counter() - cycle_started:.3f}s for {len(active)} source(s)"
                    )
                if once:
                    break
                sleep_until_next_boundary(interval_sec)
    except KeyboardInterrupt:
        emit("Stopped by user.")
    finally:
        for runtime in runtimes:
            prefix = runtime.slug if multiple_sources else ""
            try:
                completed_path = runtime.logger.close()
                if completed_path:
                    emit(f"Parquet segment saved: {completed_path}", prefix)
            except Exception as exc:
                emit(f"Failed to finalize Parquet segment: {exc}", prefix, error=True)


def track(
    input_url: str,
    api_key: str,
    interval_sec: int = 60,
    out_dir: str = DEFAULT_OUTPUT_DIR,
    scan_limit: int = 10,
    max_workers: int = 4,
    discovery_every: int = 15,
    end_confirmations: int = 2,
    row_group_rows: int = 250,
    roll_minutes: int = 60,
    once: bool = False,
    request_timeout: float = 20.0,
) -> None:
    track_many(
        [input_url],
        api_key,
        interval_sec,
        out_dir,
        scan_limit,
        max_workers,
        discovery_every,
        end_confirmations,
        row_group_rows,
        roll_minutes,
        once,
        request_timeout,
    )


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def scan_limit_value(value: str) -> int:
    parsed = positive_int(value)
    if parsed > 50:
        raise argparse.ArgumentTypeError("must be 50 or less")
    return parsed


def positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def load_url_file(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as handle:
        return [
            line.strip()
            for line in handle
            if line.strip() and not line.lstrip().startswith("#")
        ]


def load_dotenv_value(path: str, *names: str) -> Optional[str]:
    """Read selected values from a simple .env file without extra packages."""

    wanted = set(names)
    try:
        with open(path, "r", encoding="utf-8-sig") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                if line.startswith("export "):
                    line = line[7:].lstrip()
                key, value = line.split("=", 1)
                if key.strip() not in wanted:
                    continue
                value = value.strip()
                if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                    value = value[1:-1]
                if value:
                    return value
    except OSError:
        return None
    return None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("url", help="YouTube video URL, channel URL, handle, or ID")
    parser.add_argument("interval_sec", nargs="?", type=positive_int, default=60)
    parser.add_argument("out_dir", nargs="?", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("scan_limit", nargs="?", type=scan_limit_value, default=10)
    parser.add_argument(
        "--api-key",
        default=None,
        help="YouTube Data API v3 key (or set YOUTUBE_API_KEY)",
    )
    parser.add_argument(
        "--extra-url",
        action="append",
        default=[],
        help="additional YouTube URL; may be repeated",
    )
    parser.add_argument(
        "--url-file",
        action="append",
        default=[],
        help="UTF-8 text file containing one additional URL per line",
    )
    parser.add_argument("--workers", type=positive_int, default=4)
    parser.add_argument("--discovery-every", type=positive_int, default=15)
    parser.add_argument("--end-confirmations", type=positive_int, default=2)
    parser.add_argument("--row-group-rows", type=positive_int, default=250)
    parser.add_argument("--roll-minutes", type=positive_int, default=60)
    parser.add_argument("--request-timeout", type=positive_float, default=20.0)
    parser.add_argument("--once", action="store_true", help="poll once and exit")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    dotenv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    api_key = (
        args.api_key
        or os.environ.get("YOUTUBE_API_KEY")
        or os.environ.get("YOUTUBE_DATA_API_KEY")
        or load_dotenv_value(dotenv_path, "YOUTUBE_API_KEY", "YOUTUBE_DATA_API_KEY")
    )
    if not api_key or not api_key.strip():
        parser.error(
            "YouTube Data API v3 key required; set YOUTUBE_API_KEY or use --api-key"
        )

    urls = [args.url, *args.extra_url]
    try:
        for url_file in args.url_file:
            urls.extend(load_url_file(url_file))
    except OSError as exc:
        parser.error(f"could not read URL file: {exc}")

    try:
        track_many(
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
        )
    except (ValueError, YouTubeApiError, OSError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
