"""Shared HTML rendering helpers for ETL dashboards."""

from __future__ import annotations

import json
from pathlib import Path
from string import Template


def json_blob(data: dict) -> str:
    """Return compact JSON safe for inline script tags."""
    return json.dumps(data, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")


def chartjs_script(chartjs: str | None) -> str:
    return f"<script>{chartjs}</script>" if chartjs else "<script>window.Chart=null;</script>"


def render_template(template_path: Path, **values: str) -> str:
    """Render a dashboard template while tolerating unmatched JS $ tokens."""
    template_text = template_path.read_text(encoding="utf-8")
    return Template(template_text).safe_substitute(**values)
