"""Shared Chart.js cache/fetch helper for ETL dashboards."""

from __future__ import annotations

import urllib.request
from pathlib import Path


CHARTJS_URL = "https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"


def load_chartjs(cache_path: Path, fallback: str | None = None) -> str | None:
    """Return Chart.js source, downloading once into the provided cache path."""
    if cache_path.exists():
        return cache_path.read_text(encoding="utf-8")

    print("  Downloading Chart.js once...", end=" ", flush=True)
    try:
        req = urllib.request.Request(CHARTJS_URL, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=20) as response:
            js = response.read().decode("utf-8")
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(js, encoding="utf-8")
        print(f"done ({len(js) // 1024} KB cached)")
        return js
    except Exception as exc:
        print(f"failed: {exc}\n  Charts will be disabled.")
        return fallback
