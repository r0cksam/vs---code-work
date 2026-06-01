#!/usr/env python3
import os
import sys
import time
import json
import gzip
import io
import boto3
from prometheus_client import CollectorRegistry, Gauge, Counter, push_to_gateway
from datetime import datetime

ENDPOINT_URL = os.environ.get("ENDPOINT_URL")
ACCESS_KEY = os.environ.get("ACCESS_KEY_ID")
SECRET_KEY = os.environ.get("SECRET_KEY")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
PUSHGATEWAY_URL = os.environ.get("PUSHGATEWAY_URL", "pushgateway:9091")

PROCESSED_FILE = "/tmp/processed.txt"

s3_client = boto3.client(
    's3',
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name='in-maa-1'
)

registry = CollectorRegistry()

# --- Core CDN metrics ---
ttfb_gauge = Gauge('cdn_ttfb_ms', 'Time to first byte (ms)', ['cache_status', 'country'], registry=registry)
transfer_time_gauge = Gauge('cdn_transfer_time_ms', 'Transfer time (ms)', ['cache_status'], registry=registry)
cache_hits = Counter('cdn_cache_hits_total', 'Cache hit count', registry=registry)
cache_misses = Counter('cdn_cache_misses_total', 'Cache miss count', registry=registry)
errors_5xx = Counter('cdn_5xx_errors_total', '5xx errors', ['status_code'], registry=registry)
throughput_gauge = Gauge('cdn_throughput_kbps', 'Throughput (kbps)', ['cache_status'], registry=registry)

# --- Volume and session metrics ---
bytes_total = Counter('cdn_bytes_total', 'Total bytes transferred', registry=registry)
requests_total = Counter('cdn_requests_total', 'Total number of requests', registry=registry)
sess_avail_total = Counter('cdn_sess_avail_total', 'Total session available count', registry=registry)
sess_na_total = Counter('cdn_sess_na_total', 'Total session NA count', registry=registry)
sess_none_total = Counter('cdn_sess_none_total', 'Total session none count', registry=registry)

# --- Distinct IPs (active viewers) ---
distinct_ip_gauge = Gauge('cdn_distinct_ip_recent', 'Number of distinct client IPs seen in last push interval', registry=registry)
_seen_ips = set()

# --- State tracking (new) ---
requests_by_state = Counter('cdn_requests_by_state_total', 'Request count per state', ['state'], registry=registry)

def get_processed():
    if not os.path.exists(PROCESSED_FILE):
        return set()
    with open(PROCESSED_FILE, 'r') as f:
        return set(line.strip() for line in f)

def mark_processed(key):
    with open(PROCESSED_FILE, 'a') as f:
        f.write(key + "\n")

def read_log_content(body):
    if body[:2] == b'\x1f\x8b':
        with gzip.open(io.BytesIO(body), 'rt', encoding='utf-8') as gf:
            return gf.read()
    else:
        return body.decode('utf-8')

def process_log(key):
    global _seen_ips
    try:
        resp = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
        content = read_log_content(resp['Body'].read())
        lines = content.splitlines()
        if not lines:
            return

        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"JSON decode error in {key}: {e}")
                continue

            # Core fields
            ttfb = record.get('timeToFirstByte')
            transfer = record.get('transferTimeMSec')
            cache = record.get('cacheStatus')
            status = record.get('statusCode')
            country = record.get('country', 'unknown')[:2]
            obj_size = record.get('objSize', 0)

            # Additional fields
            bytes_val = record.get('bytes', 0)
            cli_ip = record.get('cliIP')
            sess_avail = record.get('sess_avail')
            sess_na = record.get('sess_na')
            sess_none = record.get('sess_none')
            state = record.get('state', 'unknown')
            if not state or state == '-':
                state = 'unknown'

            # Update counters
            requests_total.inc()
            if bytes_val:
                bytes_total.inc(float(bytes_val))
            if sess_avail:
                sess_avail_total.inc(int(sess_avail))
            if sess_na:
                sess_na_total.inc(int(sess_na))
            if sess_none:
                sess_none_total.inc(int(sess_none))
            if cli_ip:
                _seen_ips.add(cli_ip)

            # State counter
            requests_by_state.labels(state=state).inc()

            # Existing TTFB / throughput (if required fields exist)
            if ttfb is not None and transfer is not None and cache is not None and status is not None:
                try:
                    ttfb = int(ttfb)
                    transfer = int(transfer)
                    cache = str(cache)
                    status = str(status)
                    if cache == '1':
                        cache_hits.inc()
                    else:
                        cache_misses.inc()
                    if status.startswith('5'):
                        errors_5xx.labels(status_code=status).inc()
                    ttfb_gauge.labels(cache_status=cache, country=country).set(ttfb)
                    transfer_time_gauge.labels(cache_status=cache).set(transfer)
                    if obj_size and transfer > 0:
                        throughput_kbps = (obj_size * 8) / transfer
                        throughput_gauge.labels(cache_status=cache).set(throughput_kbps)
                except (ValueError, TypeError):
                    pass

        # After processing the file, update distinct IP gauge
        if _seen_ips:
            distinct_ip_gauge.set(len(_seen_ips))
            _seen_ips = set()

        push_to_gateway(PUSHGATEWAY_URL, job='cdn_logs', registry=registry)
        print(f"{datetime.now()} - Processed {key} ({len(lines)} records)")

    except Exception as e:
        print(f"Error processing {key}: {e}")

def main():
    processed = get_processed()
    print(f"Starting. Already processed {len(processed)} files.")
    while True:
        try:
            resp = s3_client.list_objects_v2(Bucket=BUCKET_NAME, MaxKeys=100)
            if 'Contents' not in resp:
                time.sleep(30)
                continue
            objects = sorted(resp['Contents'], key=lambda x: x['LastModified'])
            for obj in objects:
                key = obj['Key']
                if key not in processed:
                    process_log(key)
                    processed.add(key)
                    mark_processed(key)
        except Exception as e:
            print(f"List error: {e}")
        time.sleep(30)

if __name__ == "__main__":
    main()