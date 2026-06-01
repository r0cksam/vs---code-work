"""
lib/render.py — Assembles the final HTML dashboard from template + data blob.
"""

import json
from pathlib import Path
from string import Template
from datetime import datetime

TEMPLATE_PATH = Path(__file__).resolve().parent.parent / "template.html"


def build_data_blob(
    meta: dict,
    data_rows: list,
    device_daily: list,
    device_first_seen_counts: dict,
    device_summary: dict,
    data_time_range: str,
    generated_at: datetime,
) -> str:
    return json.dumps(
        {
            "meta":                    meta,
            "rows":                    data_rows,
            "device_daily":            device_daily,
            "device_first_seen_counts":device_first_seen_counts,
            "device_summary":          device_summary,
            "generated":               generated_at.strftime("%Y-%m-%d %H:%M:%S"),
            "report_date":             generated_at.strftime("%Y-%m-%d"),
            "data_range":              data_time_range,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )


def render_html(data_blob: str, chartjs: str | None) -> str:
    """Substitute data and Chart.js into the HTML template."""
    chartjs_tag = (
        f"<script>{chartjs}</script>" if chartjs else "<script>window.Chart=null;</script>"
    )
    template_text = TEMPLATE_PATH.read_text(encoding="utf-8")
    # Using safe_substitute so any un-matched $-tokens in JS don't raise errors
    return Template(template_text).safe_substitute(
        DATA_BLOB=data_blob,
        CHARTJS_TAG=chartjs_tag,
    )
