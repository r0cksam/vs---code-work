"""
YouTube Live Viewer Count Tracker (No API Key Needed) -- Parquet Edition
--------------------------------------------------------------------------
Uses yt-dlp to read publicly visible live viewer counts. It can track one or
many video/channel URLs in a single process.

Channel polling is deliberately conservative:
    - The channel's Streams tab is scanned on a configurable cadence.
    - Flat-list live status/duration signals avoid full lookups of ended streams.
    - Full video lookups run in a small, bounded worker pool.
    - A failed scan never turns every known stream into a false "ended" event.
    - A stream must be confirmed non-live on multiple polls before it ends.

Storage:
    Rows are buffered into useful Parquet row groups. Files roll hourly by
    default, so completed segments remain readable even if a later segment is
    interrupted. The active file has a .partial suffix and is atomically
    renamed to .parquet after it is closed successfully.

    With multiple sources, each URL is written below its own
    output/source=<name-hash>/ directory so schemas and file publication stay
    independent while all sources share one globally bounded lookup pool.

    Query all completed segments with DuckDB:
        SELECT *
        FROM read_parquet('viewer_logs/*.parquet')
        ORDER BY date, time;

    For a multi-source run, the source name is inferred from its Hive-style
    directory:
        SELECT *
        FROM read_parquet(
            'viewer_logs/source=*/*.parquet',
            hive_partitioning = true
        )
        ORDER BY source, date, time;

Setup:
    pip install -U yt-dlp pyarrow

Usage (the original positional arguments remain compatible):
    python YT.py URL [interval_sec] [out_dir] [scan_limit] [options]

Examples:
    python YT.py https://www.youtube.com/watch?v=VIDEO_ID 60
    python YT.py https://www.youtube.com/@indiatv 90
    python YT.py https://www.youtube.com/@indiatv 90 --workers 4 --once
    python YT.py https://www.youtube.com/@indiatv 90 --extra-url https://www.youtube.com/@aajtak
    python YT.py https://www.youtube.com/@indiatv 90 --url-file channels.txt

Useful options:
    --extra-url URL       Add another channel/video (repeatable).
    --url-file PATH       Add URLs from a text file, one per line (repeatable).
    --discovery-every N   Scan the channel tab every N polls (default: 1).
    --workers N           Maximum parallel video lookups (default: 4).
    --end-confirmations N Successful non-live checks required (default: 2).
    --row-group-rows N    Buffered rows per Parquet row group (default: 250).
    --roll-minutes N      Close and publish a file every N minutes (default: 60).
    --once                Run one poll, print its timing, and exit.

Note: yt-dlp relies on YouTube's public page structure rather than a versioned
official API, so extraction can break when YouTube changes its site.
"""

import argparse
import datetime
import hashlib
import os
import re
import sys
import threading
import time
import uuid
from concurrent.futures import Executor, ThreadPoolExecutor
from contextlib import nullcontext
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq
import yt_dlp
from yt_dlp.utils import DownloadError

# Avoid crashes when printing non-English characters (for example, Hindi titles)
# in Windows terminals.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30), name="IST")

YDL_OPTS = {
    "quiet": True,
    "no_warnings": True,
    "skip_download": True,
}

CHANNEL_MARKERS = ("/@", "/channel/", "/c/", "/user/")
ENDED_STATUSES = {"was_live", "post_live", "not_live"}
DIRECT_STOP_STATUSES = {"was_live", "post_live"}
PRINT_LOCK = threading.Lock()

SCHEMA = pa.schema([
    ("date", pa.string()),       # YYYY-MM-DD in IST; lexically sortable
    ("time", pa.string()),       # HH:MM:SS in IST
    ("video_id", pa.string()),
    ("title", pa.string()),
    ("concurrent_viewers", pa.int64()),
    ("status", pa.string()),
])

Row = Tuple[str, str, Optional[str], Optional[str], Optional[int], str]


@dataclass(frozen=True)
class VideoLookup:
    video_id: Optional[str]
    title: Optional[str]
    viewers: Optional[int]
    status: str
    error: Optional[str] = None


@dataclass(frozen=True)
class CandidateVideo:
    video_id: str
    title: Optional[str]
    status: Optional[str]


@dataclass
class ChannelState:
    active_ids: Set[str] = field(default_factory=set)
    known_titles: Dict[str, Optional[str]] = field(default_factory=dict)
    end_misses: Dict[str, int] = field(default_factory=dict)
    terminal_ids: Set[str] = field(default_factory=set)
    poll_number: int = 0


@dataclass(frozen=True)
class PollOutcome:
    rows: List[Row]
    discovery_calls: int
    video_calls: int
    should_stop: bool
    elapsed: float


def emit(message: str, prefix: str = "", error: bool = False) -> None:
    """Keep concurrent source output readable one complete line at a time."""

    rendered = f"[{prefix}] {message}" if prefix else message
    with PRINT_LOCK:
        print(rendered, file=sys.stderr if error else sys.stdout)


def is_channel_url(url: str) -> bool:
    return any(marker in url for marker in CHANNEL_MARKERS) and "watch?v=" not in url


def _normalise_viewer_count(value) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def get_video_status(
    video_url: str,
    ydl=None,
    fallback_video_id: Optional[str] = None,
    request_gate: Optional[threading.BoundedSemaphore] = None,
) -> VideoLookup:
    """Return a structured lookup result without turning network errors into ends."""

    try:
        if ydl is None:
            with yt_dlp.YoutubeDL(YDL_OPTS) as local_ydl:
                with request_gate or nullcontext():
                    info = local_ydl.extract_info(video_url, download=False)
        else:
            with request_gate or nullcontext():
                info = ydl.extract_info(video_url, download=False)
    except DownloadError as exc:
        return VideoLookup(fallback_video_id, None, None, "lookup_error", str(exc))
    except Exception as exc:
        return VideoLookup(fallback_video_id, None, None, "lookup_error", str(exc))

    return VideoLookup(
        info.get("id") or fallback_video_id,
        info.get("title"),
        _normalise_viewer_count(info.get("concurrent_view_count")),
        info.get("live_status") or "unknown",
    )


def get_channel_streams_tab_url(channel_url: str) -> str:
    base = channel_url.rstrip("/")
    for suffix in ("/live", "/streams", "/videos", "/shorts", "/featured"):
        if base.endswith(suffix):
            base = base[: -len(suffix)]
            break
    return base + "/streams"


def list_candidate_videos(
    channel_url: str,
    limit: int,
    request_gate: Optional[threading.BoundedSemaphore] = None,
) -> List[CandidateVideo]:
    """Cheaply list recent entries and retain flat metadata when available."""

    opts = dict(YDL_OPTS)
    opts["extract_flat"] = True
    opts["playlistend"] = limit
    with yt_dlp.YoutubeDL(opts) as ydl:
        with request_gate or nullcontext():
            info = ydl.extract_info(get_channel_streams_tab_url(channel_url), download=False)

    candidates = []
    for entry in info.get("entries") or []:
        if not entry or not entry.get("id"):
            continue
        status = entry.get("live_status")
        # Some YouTube channel payloads omit live_status entirely. A concrete
        # duration in the Streams tab is still a safe cheap signal that the
        # entry is a completed VOD, while active/upcoming streams have no fixed
        # duration. Previously-active IDs are always fully checked regardless.
        if status is None and entry.get("duration") is not None:
            status = "not_live"
        candidates.append(
            CandidateVideo(str(entry["id"]), entry.get("title"), status)
        )
    return candidates


def _lookup_video_chunk(
    video_ids: Sequence[str],
    request_gate: Optional[threading.BoundedSemaphore] = None,
) -> Dict[str, VideoLookup]:
    """Use one YoutubeDL instance for a sequential chunk handled by one worker."""

    results: Dict[str, VideoLookup] = {}
    try:
        with yt_dlp.YoutubeDL(YDL_OPTS) as ydl:
            for video_id in video_ids:
                url = f"https://www.youtube.com/watch?v={video_id}"
                results[video_id] = get_video_status(
                    url,
                    ydl,
                    video_id,
                    request_gate,
                )
    except Exception as exc:
        # Construction/context-manager failures happen outside get_video_status.
        for video_id in video_ids:
            results.setdefault(
                video_id,
                VideoLookup(video_id, None, None, "lookup_error", str(exc)),
            )
    return results


def lookup_video_ids(
    video_ids: Iterable[str],
    max_workers: int,
    executor: Optional[Executor] = None,
    request_gate: Optional[threading.BoundedSemaphore] = None,
) -> Dict[str, VideoLookup]:
    """Look up unique IDs with bounded parallelism and one yt-dlp instance per worker."""

    unique_ids = list(dict.fromkeys(video_ids))
    if not unique_ids:
        return {}

    worker_count = min(max_workers, len(unique_ids))
    if worker_count == 1 and executor is None:
        return _lookup_video_chunk(unique_ids, request_gate)

    chunks = [unique_ids[index::worker_count] for index in range(worker_count)]
    results: Dict[str, VideoLookup] = {}
    if executor is not None:
        futures = [executor.submit(_lookup_video_chunk, chunk, request_gate) for chunk in chunks]
        for future in futures:
            results.update(future.result())
    else:
        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="yt-lookup") as pool:
            for chunk_result in pool.map(_lookup_video_chunk, chunks):
                results.update(chunk_result)
    return results


def sleep_until_next_boundary(interval_sec: int) -> None:
    """Sleep until the next clean wall-clock interval boundary."""

    now = time.time()
    time.sleep(interval_sec - (now % interval_sec))


def write_batch(writer: pq.ParquetWriter, rows: Sequence[Row]) -> None:
    """Write one typed Parquet row group."""

    if not rows:
        return
    columns = list(zip(*rows))
    arrays = [
        pa.array(column, type=field.type)
        for column, field in zip(columns, SCHEMA)
    ]
    writer.write_table(pa.Table.from_arrays(arrays, schema=SCHEMA))


class RollingParquetLogger:
    """Buffer useful row groups and atomically publish closed rolling files."""

    def __init__(self, out_dir: str, row_group_rows: int, roll_minutes: int):
        self.out_dir = out_dir
        self.row_group_rows = row_group_rows
        self.roll_seconds = roll_minutes * 60
        self.buffer: List[Row] = []
        self.writer: Optional[pq.ParquetWriter] = None
        self.partial_path: Optional[str] = None
        self.final_path: Optional[str] = None
        self.opened_at = 0.0
        os.makedirs(out_dir, exist_ok=True)
        self._open_segment()

    def _open_segment(self) -> None:
        stamp = datetime.datetime.now(IST).strftime("%Y-%m-%d_%H%M%S")
        token = uuid.uuid4().hex[:8]
        filename = f"log_{stamp}_{token}.parquet"
        self.final_path = os.path.join(self.out_dir, filename)
        self.partial_path = self.final_path + ".partial"
        self.writer = pq.ParquetWriter(self.partial_path, SCHEMA)
        self.opened_at = time.monotonic()

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
        """Add rows and return a completed path when a rotation occurred."""

        completed_path = None
        if time.monotonic() - self.opened_at >= self.roll_seconds:
            completed_path = self._close_segment()
            self._open_segment()
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
    channel_url: str,
    now_ist: datetime.datetime,
    state: ChannelState,
    scan_limit: int,
    max_workers: int,
    discovery_every: int,
    end_confirmations: int,
    executor: Optional[Executor] = None,
    request_gate: Optional[threading.BoundedSemaphore] = None,
    log_prefix: str = "",
) -> Tuple[List[Row], int, int]:
    """Run one channel poll and update state without false transitions on errors."""

    state.poll_number += 1
    discovery_due = (state.poll_number - 1) % discovery_every == 0
    discovery_calls = 0
    scan_error: Optional[str] = None
    candidates: List[CandidateVideo] = []
    flat_statuses: Dict[str, Optional[str]] = {}

    if discovery_due:
        discovery_calls = 1
        try:
            candidates = list_candidate_videos(channel_url, scan_limit, request_gate)
            flat_statuses = {candidate.video_id: candidate.status for candidate in candidates}
        except Exception as exc:
            scan_error = str(exc)
            emit(
                f"{now_ist:%Y-%m-%d %H:%M:%S} IST  Channel scan error: {exc}",
                log_prefix,
            )

    lookup_ids = sorted(state.active_ids)
    seen_lookup_ids = set(lookup_ids)

    for candidate in candidates:
        if candidate.title:
            state.known_titles[candidate.video_id] = candidate.title

        if candidate.status in ENDED_STATUSES:
            state.terminal_ids.add(candidate.video_id)
        elif candidate.status in {"is_live", "is_upcoming"}:
            state.terminal_ids.discard(candidate.video_id)

        should_lookup = (
            candidate.video_id in state.active_ids
            or candidate.status == "is_live"
            or (
                candidate.status not in ENDED_STATUSES | {"is_upcoming"}
                and candidate.video_id not in state.terminal_ids
            )
        )
        if should_lookup and candidate.video_id not in seen_lookup_ids:
            lookup_ids.append(candidate.video_id)
            seen_lookup_ids.add(candidate.video_id)

    results = lookup_video_ids(lookup_ids, max_workers, executor, request_gate)
    previous_active = set(state.active_ids)
    next_active: Set[str] = set()
    confirmed_live: Set[str] = set()
    rows: List[Row] = []

    for video_id in lookup_ids:
        result = results.get(
            video_id,
            VideoLookup(video_id, None, None, "lookup_error", "missing worker result"),
        )
        title = result.title or state.known_titles.get(video_id)
        if title:
            state.known_titles[video_id] = title

        # A successful flat scan can confirm that an otherwise unreachable
        # previously-live video is now terminal.
        effective_status = result.status
        if result.status == "lookup_error" and flat_statuses.get(video_id) in ENDED_STATUSES:
            effective_status = flat_statuses[video_id] or "not_live"

        if effective_status == "is_live":
            next_active.add(video_id)
            confirmed_live.add(video_id)
            state.end_misses.pop(video_id, None)
            state.terminal_ids.discard(video_id)
            rows.append(make_row(now_ist, video_id, title, result.viewers, "is_live"))
            continue

        if video_id in previous_active:
            if effective_status == "lookup_error":
                # Preserve active state across transient lookup failures.
                next_active.add(video_id)
                rows.append(make_row(now_ist, video_id, title, None, "lookup_error"))
                emit(
                    f"!! [{video_id}] lookup failed; keeping prior live state: {result.error}",
                    log_prefix,
                )
                continue

            misses = state.end_misses.get(video_id, 0) + 1
            state.end_misses[video_id] = misses
            if misses >= end_confirmations:
                state.end_misses.pop(video_id, None)
                if effective_status in ENDED_STATUSES:
                    state.terminal_ids.add(video_id)
                rows.append(make_row(now_ist, video_id, title, None, "stream_ended"))
                emit(f'>> STREAM ENDED: [{video_id}] "{title}"', log_prefix)
            else:
                next_active.add(video_id)
                rows.append(make_row(now_ist, video_id, title, None, "end_pending"))
                emit(
                    f"?? [{video_id}] non-live confirmation "
                    f"{misses}/{end_confirmations} ({effective_status})",
                    log_prefix,
                )
            continue

        if effective_status == "lookup_error":
            rows.append(make_row(now_ist, video_id, title, None, "lookup_error"))
            emit(f"!! [{video_id}] candidate lookup failed: {result.error}", log_prefix)
        elif effective_status in ENDED_STATUSES:
            state.terminal_ids.add(video_id)

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
        if scan_error:
            rows.append(make_row(now_ist, None, None, None, "channel_scan_error"))
        else:
            rows.append(make_row(now_ist, None, None, None, "no_live_streams"))
            if discovery_due:
                emit(
                    f"{now_ist:%Y-%m-%d %H:%M:%S} IST  No live streams found.",
                    log_prefix,
                )
            else:
                emit(
                    f"{now_ist:%Y-%m-%d %H:%M:%S} IST  No known live streams.",
                    log_prefix,
                )

    return rows, discovery_calls, len(lookup_ids)


def _require_positive(name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero (got {value})")


@dataclass
class SourceRuntime:
    url: str
    slug: str
    channel_mode: bool
    logger: RollingParquetLogger
    channel_state: ChannelState = field(default_factory=ChannelState)
    stopped: bool = False


def source_slug(url: str) -> str:
    parsed = urlparse(url)
    path_parts = [part.lstrip("@") for part in parsed.path.split("/") if part]
    human_part = path_parts[-1] if path_parts else parsed.netloc or "youtube"
    safe_part = re.sub(r"[^A-Za-z0-9._-]+", "-", human_part).strip("-._")
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:8]
    return f"{(safe_part or 'source')[:48]}-{digest}"


def poll_source(
    runtime: SourceRuntime,
    scan_limit: int,
    max_workers: int,
    discovery_every: int,
    end_confirmations: int,
    lookup_executor: Executor,
    request_gate: threading.BoundedSemaphore,
    log_prefix: str,
) -> PollOutcome:
    poll_started = time.perf_counter()
    now_ist = datetime.datetime.now(IST)

    if runtime.channel_mode:
        rows, discovery_calls, video_calls = poll_channel(
            runtime.url,
            now_ist,
            runtime.channel_state,
            scan_limit,
            max_workers,
            discovery_every,
            end_confirmations,
            lookup_executor,
            request_gate,
            log_prefix,
        )
        should_stop = False
    else:
        result = get_video_status(runtime.url, request_gate=request_gate)
        rows = [
            make_row(
                now_ist,
                result.video_id,
                result.title,
                result.viewers,
                result.status,
            )
        ]
        discovery_calls = 0
        video_calls = 1
        should_stop = result.status in DIRECT_STOP_STATUSES
        if result.status == "lookup_error":
            emit(
                f"{now_ist:%Y-%m-%d %H:%M:%S} IST  Lookup error: {result.error}",
                log_prefix,
            )
        else:
            emit(
                f"{now_ist:%Y-%m-%d %H:%M:%S} IST  "
                f"viewers={result.viewers}  status={result.status}",
                log_prefix,
            )

    return PollOutcome(
        rows,
        discovery_calls,
        video_calls,
        should_stop,
        time.perf_counter() - poll_started,
    )


def track_many(
    input_urls: Sequence[str],
    interval_sec: int = 60,
    out_dir: str = "viewer_logs",
    scan_limit: int = 10,
    max_workers: int = 4,
    discovery_every: int = 1,
    end_confirmations: int = 2,
    row_group_rows: int = 250,
    roll_minutes: int = 60,
    once: bool = False,
) -> None:
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

    urls = list(dict.fromkeys(url.strip() for url in input_urls if url.strip()))
    if not urls:
        raise ValueError("at least one YouTube URL is required")

    multiple_sources = len(urls) > 1
    runtimes: List[SourceRuntime] = []
    try:
        for url in urls:
            slug = source_slug(url)
            source_out_dir = (
                os.path.join(out_dir, f"source={slug}")
                if multiple_sources
                else out_dir
            )
            runtimes.append(
                SourceRuntime(
                    url,
                    slug,
                    is_channel_url(url),
                    RollingParquetLogger(source_out_dir, row_group_rows, roll_minutes),
                )
            )
    except Exception:
        for runtime in runtimes:
            runtime.logger.close()
        raise

    emit(
        f"Tracking {len(runtimes)} source(s) every {interval_sec}s with "
        f"a global limit of {max_workers} concurrent extraction(s). "
        "Press Ctrl+C to stop."
    )
    for runtime in runtimes:
        prefix = runtime.slug if multiple_sources else ""
        emit(f"Source: {runtime.url}", prefix)
        emit(f"Writing active segment to: {runtime.logger.partial_path}", prefix)

    request_gate = threading.BoundedSemaphore(max_workers)
    source_worker_count = min(len(runtimes), max_workers)

    try:
        with (
            ThreadPoolExecutor(
                max_workers=max_workers,
                thread_name_prefix="yt-global-lookup",
            ) as lookup_executor,
            ThreadPoolExecutor(
                max_workers=source_worker_count,
                thread_name_prefix="yt-source-poll",
            ) as source_executor,
        ):
            while True:
                active_runtimes = [runtime for runtime in runtimes if not runtime.stopped]
                if not active_runtimes:
                    break

                cycle_started = time.perf_counter()
                future_map = {
                    source_executor.submit(
                        poll_source,
                        runtime,
                        scan_limit,
                        max_workers,
                        discovery_every,
                        end_confirmations,
                        lookup_executor,
                        request_gate,
                        runtime.slug if multiple_sources else "",
                    ): runtime
                    for runtime in active_runtimes
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
                            False,
                            time.perf_counter() - cycle_started,
                        )

                    completed_path = runtime.logger.add_rows(outcome.rows)
                    if completed_path:
                        emit(f"Parquet segment saved: {completed_path}", prefix)
                        emit(
                            f"Writing active segment to: {runtime.logger.partial_path}",
                            prefix,
                        )

                    emit(
                        f"Poll benchmark: {outcome.elapsed:.3f}s, "
                        f"extractor_calls={outcome.discovery_calls + outcome.video_calls} "
                        f"(discovery={outcome.discovery_calls}, "
                        f"videos={outcome.video_calls}), rows={len(outcome.rows)}",
                        prefix,
                    )

                    if outcome.should_stop:
                        runtime.stopped = True
                        emit("Stream has ended. Stopping this source.", prefix)

                cycle_elapsed = time.perf_counter() - cycle_started
                if multiple_sources:
                    emit(
                        f"Global cycle benchmark: {cycle_elapsed:.3f}s for "
                        f"{len(active_runtimes)} source(s)"
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
    interval_sec: int = 60,
    out_dir: str = "viewer_logs",
    scan_limit: int = 10,
    max_workers: int = 4,
    discovery_every: int = 1,
    end_confirmations: int = 2,
    row_group_rows: int = 250,
    roll_minutes: int = 60,
    once: bool = False,
) -> None:
    """Backward-compatible single-source entry point."""

    track_many(
        [input_url],
        interval_sec,
        out_dir,
        scan_limit,
        max_workers,
        discovery_every,
        end_confirmations,
        row_group_rows,
        roll_minutes,
        once,
    )


def positive_int(value: str) -> int:
    parsed = int(value)
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("url", help="YouTube video or channel URL")
    parser.add_argument("interval_sec", nargs="?", type=positive_int, default=60)
    parser.add_argument("out_dir", nargs="?", default="viewer_logs")
    parser.add_argument("scan_limit", nargs="?", type=positive_int, default=10)
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
    parser.add_argument("--discovery-every", type=positive_int, default=1)
    parser.add_argument("--end-confirmations", type=positive_int, default=2)
    parser.add_argument("--row-group-rows", type=positive_int, default=250)
    parser.add_argument("--roll-minutes", type=positive_int, default=60)
    parser.add_argument("--once", action="store_true", help="poll once and exit")
    return parser


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()
    urls = [args.url, *args.extra_url]
    try:
        for url_file in args.url_file:
            urls.extend(load_url_file(url_file))
    except OSError as exc:
        parser.error(f"could not read URL file: {exc}")

    track_many(
        urls,
        args.interval_sec,
        args.out_dir,
        args.scan_limit,
        args.workers,
        args.discovery_every,
        args.end_confirmations,
        args.row_group_rows,
        args.roll_minutes,
        args.once,
    )
