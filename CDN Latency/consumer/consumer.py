#!/usr/bin/env python3
import gzip
import io
import json
import logging
import os
import socket
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, push_to_gateway


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
LOG = logging.getLogger("cdn-consumer")


def env_required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


ENDPOINT_URL = env_required("ENDPOINT_URL")
ACCESS_KEY = env_required("ACCESS_KEY_ID")
SECRET_KEY = env_required("SECRET_KEY")
BUCKET_NAME = env_required("BUCKET_NAME")
PUSHGATEWAY_URL = os.environ.get("PUSHGATEWAY_URL", "pushgateway:9091").strip()
POLL_INTERVAL_SEC = int(os.environ.get("POLL_INTERVAL_SEC", "30"))
PROCESSED_FILE = Path(os.environ.get("PROCESSED_FILE", "/state/processed_keys.txt"))
S3_PREFIX = os.environ.get("S3_PREFIX", "").strip()
S3_REGION = os.environ.get("S3_REGION", "in-maa-1").strip()
LOG_KEY_SUFFIXES = tuple(
    suffix.strip() for suffix in os.environ.get("LOG_KEY_SUFFIXES", ".gz").split(",") if suffix.strip()
)

s3_client = boto3.client(
    "s3",
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name=S3_REGION,
)

REGISTRY = CollectorRegistry()

# Core throughput and latency distributions (safer than per-record gauges).
ttfb_hist = Histogram(
    "cdn_ttfb_ms",
    "TTFB in milliseconds",
    ["cache_status"],
    buckets=(5, 10, 25, 50, 100, 200, 400, 800, 1600, 3200, 6400),
    registry=REGISTRY,
)
transfer_hist = Histogram(
    "cdn_transfer_time_ms",
    "Transfer time in milliseconds",
    ["cache_status"],
    buckets=(25, 50, 100, 200, 400, 800, 1600, 3200, 6400, 12800),
    registry=REGISTRY,
)
throughput_hist = Histogram(
    "cdn_throughput_kbps",
    "Throughput in kbps",
    ["cache_status"],
    buckets=(50, 100, 250, 500, 1000, 2500, 5000, 10000, 25000),
    registry=REGISTRY,
)

# Volume and request counters.
requests_total = Counter("cdn_requests_total", "Total request rows processed", registry=REGISTRY)
records_processed_total = Counter("cdn_records_processed_total", "Total JSON records parsed", registry=REGISTRY)
bytes_total = Counter("cdn_bytes_total", "Total bytes transferred", registry=REGISTRY)
files_processed_total = Counter("cdn_files_processed_total", "Total object files processed", registry=REGISTRY)

# Session and request quality counters.
sess_avail_total = Counter("cdn_sess_avail_total", "Total session available count", registry=REGISTRY)
sess_na_total = Counter("cdn_sess_na_total", "Total session NA count", registry=REGISTRY)
sess_none_total = Counter("cdn_sess_none_total", "Total session none count", registry=REGISTRY)
cache_hits = Counter("cdn_cache_hits_total", "Cache hit count", registry=REGISTRY)
cache_misses = Counter("cdn_cache_misses_total", "Cache miss count", registry=REGISTRY)
cache_requests_total = Counter(
    "cdn_cache_requests_total",
    "Request count by cache status",
    ["cache_status"],
    registry=REGISTRY,
)
status_requests_total = Counter(
    "cdn_status_requests_total",
    "Request count by HTTP status",
    ["status_code"],
    registry=REGISTRY,
)
errors_5xx = Counter("cdn_5xx_errors_total", "5xx response count", ["status_code"], registry=REGISTRY)
requests_by_state = Counter("cdn_requests_by_state_total", "Request count per state", ["state"], registry=REGISTRY)
requests_by_country = Counter(
    "cdn_requests_by_country_total",
    "Request count per country code",
    ["country"],
    registry=REGISTRY,
)

# Interval gauges and service health.
distinct_ip_recent = Gauge(
    "cdn_distinct_ip_recent",
    "Distinct client IPs seen during latest consumer poll cycle",
    registry=REGISTRY,
)
consumer_last_success_unixtime = Gauge(
    "cdn_consumer_last_success_unixtime",
    "Unix timestamp for last successful poll cycle",
    registry=REGISTRY,
)
consumer_last_error_unixtime = Gauge(
    "cdn_consumer_last_error_unixtime",
    "Unix timestamp for most recent poll error",
    registry=REGISTRY,
)
consumer_cycle_seconds = Histogram(
    "cdn_consumer_cycle_seconds",
    "Duration of one consumer poll cycle",
    buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60, 120),
    registry=REGISTRY,
)


def as_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def as_int(value) -> int | None:
    f = as_float(value)
    return None if f is None else int(f)


def read_log_content(body: bytes) -> str:
    if body[:2] == b"\x1f\x8b":
        with gzip.open(io.BytesIO(body), "rt", encoding="utf-8", errors="replace") as gz:
            return gz.read()
    return body.decode("utf-8", errors="replace")


def load_processed_keys() -> set[str]:
    if not PROCESSED_FILE.exists():
        return set()
    return {line.strip() for line in PROCESSED_FILE.read_text(encoding="utf-8").splitlines() if line.strip()}


def mark_processed(key: str) -> None:
    PROCESSED_FILE.parent.mkdir(parents=True, exist_ok=True)
    with PROCESSED_FILE.open("a", encoding="utf-8") as f:
        f.write(f"{key}\n")


def iter_new_object_keys(processed: set[str]) -> Iterator[str]:
    paginator = s3_client.get_paginator("list_objects_v2")
    args = {"Bucket": BUCKET_NAME}
    if S3_PREFIX:
        args["Prefix"] = S3_PREFIX

    page_count = 0
    yielded = 0
    for page in paginator.paginate(**args, PaginationConfig={"PageSize": 1000}):
        page_count += 1
        page_pending: list[tuple[datetime, str]] = []
        for obj in page.get("Contents", []):
            key = obj.get("Key")
            if not key or key.endswith("/") or key in processed:
                continue
            if LOG_KEY_SUFFIXES and not key.endswith(LOG_KEY_SUFFIXES):
                continue
            last_modified = obj.get("LastModified") or datetime.min.replace(tzinfo=timezone.utc)
            page_pending.append((last_modified, key))

        # Sort only this page and yield immediately so we can start processing fast.
        page_pending.sort(key=lambda item: item[0])
        if page_pending:
            LOG.info("S3 list page=%d new_keys=%d", page_count, len(page_pending))
        for _, key in page_pending:
            yielded += 1
            yield key

    LOG.info("S3 list complete. pages=%d yielded_new_keys=%d", page_count, yielded)


def process_log_object(key: str) -> tuple[int, set[str]]:
    resp = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
    content = read_log_content(resp["Body"].read())
    ips_seen: set[str] = set()
    records = 0

    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue

        records += 1
        records_processed_total.inc()
        requests_total.inc()

        bytes_val = as_float(record.get("bytes"))
        if bytes_val and bytes_val > 0:
            bytes_total.inc(bytes_val)

        sess_avail = as_int(record.get("sess_avail"))
        sess_na = as_int(record.get("sess_na"))
        sess_none = as_int(record.get("sess_none"))
        if sess_avail:
            sess_avail_total.inc(sess_avail)
        if sess_na:
            sess_na_total.inc(sess_na)
        if sess_none:
            sess_none_total.inc(sess_none)

        cli_ip = str(record.get("cliIP", "")).strip()
        if cli_ip:
            ips_seen.add(cli_ip)

        state = str(record.get("state", "unknown")).strip() or "unknown"
        if state == "-":
            state = "unknown"
        requests_by_state.labels(state=state).inc()

        country = str(record.get("country", "unknown")).strip()[:2].lower() or "unknown"
        requests_by_country.labels(country=country).inc()

        cache_status = str(record.get("cacheStatus", "unknown")).strip() or "unknown"
        status_code = str(record.get("statusCode", "unknown")).strip() or "unknown"

        cache_requests_total.labels(cache_status=cache_status).inc()
        status_requests_total.labels(status_code=status_code).inc()

        if cache_status == "1":
            cache_hits.inc()
        else:
            cache_misses.inc()

        if status_code.startswith("5"):
            errors_5xx.labels(status_code=status_code).inc()

        ttfb = as_float(record.get("timeToFirstByte"))
        if ttfb is not None and ttfb >= 0:
            ttfb_hist.labels(cache_status=cache_status).observe(ttfb)

        transfer = as_float(record.get("transferTimeMSec"))
        if transfer is not None and transfer > 0:
            transfer_hist.labels(cache_status=cache_status).observe(transfer)

            obj_size = as_float(record.get("objSize"))
            if obj_size is not None and obj_size > 0:
                throughput_kbps = (obj_size * 8.0) / transfer
                throughput_hist.labels(cache_status=cache_status).observe(throughput_kbps)

    files_processed_total.inc()
    LOG.info("Processed %s (%d records)", key, records)
    return records, ips_seen


def push_metrics() -> None:
    push_to_gateway(
        PUSHGATEWAY_URL,
        job="cdn_logs",
        registry=REGISTRY,
        grouping_key={"instance": socket.gethostname()},
    )


def main() -> None:
    processed = load_processed_keys()
    LOG.info(
        "Consumer started. Processed key cache size=%d prefix=%r suffixes=%r poll_sec=%d",
        len(processed),
        S3_PREFIX,
        LOG_KEY_SUFFIXES,
        POLL_INTERVAL_SEC,
    )

    while True:
        cycle_start = time.monotonic()
        cycle_ips: set[str] = set()
        new_files = 0
        new_records = 0
        files_since_push = 0

        try:
            for key in iter_new_object_keys(processed):
                records, ips = process_log_object(key)
                processed.add(key)
                mark_processed(key)
                cycle_ips.update(ips)
                new_files += 1
                new_records += records
                files_since_push += 1

                # During backlog catch-up, push periodically so Grafana shows data quickly.
                if files_since_push >= 50:
                    distinct_ip_recent.set(len(cycle_ips))
                    consumer_last_success_unixtime.set(time.time())
                    push_metrics()
                    files_since_push = 0

            distinct_ip_recent.set(len(cycle_ips))
            consumer_last_success_unixtime.set(time.time())

        except (ClientError, BotoCoreError, OSError, RuntimeError) as exc:
            consumer_last_error_unixtime.set(time.time())
            LOG.exception("Poll cycle failed: %s", exc)

        finally:
            consumer_cycle_seconds.observe(time.monotonic() - cycle_start)
            try:
                push_metrics()
            except Exception as exc:  # noqa: BLE001
                LOG.exception("Pushgateway push failed: %s", exc)

        LOG.info("Cycle complete. New files=%d records=%d", new_files, new_records)

        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    main()
