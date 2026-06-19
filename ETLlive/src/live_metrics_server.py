#!/usr/bin/env python3
"""Tiny local HTTP server for ETLlive Prometheus metrics and health."""

from __future__ import annotations

import argparse
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from live_common import DEFAULT_CONFIG, load_config, resolve_live_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Serve ETLlive metrics for Prometheus/Grafana testing.")
    parser.add_argument("--config", type=Path, default=Path(os.getenv("VETO_LIVE_CONFIG", str(DEFAULT_CONFIG))))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9108)
    return parser.parse_args()


class Handler(BaseHTTPRequestHandler):
    metrics_path: Path
    health_path: Path

    def do_GET(self) -> None:  # noqa: N802
        if self.path.split("?", 1)[0] == "/metrics":
            self._send_file(self.metrics_path, "text/plain; version=0.0.4; charset=utf-8")
            return
        if self.path.split("?", 1)[0] == "/health":
            self._send_file(self.health_path, "application/json; charset=utf-8")
            return
        self.send_response(404)
        self.end_headers()
        self.wfile.write(b"not found\n")

    def log_message(self, format: str, *args) -> None:  # noqa: A002
        return

    def _send_file(self, path: Path, content_type: str) -> None:
        if not path.exists():
            self.send_response(404)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(f"missing file: {path}\n".encode("utf-8"))
            return
        payload = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    output = config.get("output") or {}
    Handler.metrics_path = resolve_live_path(output.get("metrics_prom", "output/live_metrics.prom"))
    Handler.health_path = resolve_live_path(output.get("health_json", "output/live_health.json"))
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"Serving ETLlive metrics at http://{args.host}:{args.port}/metrics")
    print(f"Serving ETLlive health at  http://{args.host}:{args.port}/health")
    server.serve_forever()


if __name__ == "__main__":
    main()
