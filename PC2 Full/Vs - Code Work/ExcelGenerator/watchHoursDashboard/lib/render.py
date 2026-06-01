"""
lib/render.py — Assembles the final HTML from template.html and the data blob.
"""

from __future__ import annotations

import json
from pathlib import Path
from string import Template

TEMPLATE_PATH = Path(__file__).resolve().parent.parent / "template.html"


def build_data_blob(data: dict) -> str:
    """Serialise the report dict to a compact JSON string safe for inline <script>."""
    return json.dumps(data, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")


def render_html(data: dict, chartjs: str) -> str:
    """Substitute data blob and Chart.js into the HTML template."""
    data_blob = build_data_blob(data)
    template_text = TEMPLATE_PATH.read_text(encoding="utf-8")
    return Template(template_text).safe_substitute(
        DATA_BLOB=data_blob,
        CHARTJS=chartjs,
    )
