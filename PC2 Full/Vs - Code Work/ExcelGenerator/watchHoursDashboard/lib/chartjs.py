"""
lib/chartjs.py — Chart.js bundle fetcher with local cache.
"""

import urllib.request
from pathlib import Path

CHARTJS_URL   = "https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"
CHARTJS_CACHE = Path(__file__).resolve().parent.parent / ".chartjs_cache.js"


def get_chartjs() -> str | None:
    """Return Chart.js source, downloading once and caching locally."""
    if CHARTJS_CACHE.exists():
        return CHARTJS_CACHE.read_text(encoding="utf-8")
    print("  Downloading Chart.js once...", end=" ", flush=True)
    try:
        req = urllib.request.Request(CHARTJS_URL, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            js = r.read().decode("utf-8")
        CHARTJS_CACHE.write_text(js, encoding="utf-8")
        print(f"done ({len(js) // 1024} KB cached)")
        return js
    except Exception as e:
        print(f"failed: {e}\n  Charts will be disabled.")
        return None
