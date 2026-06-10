from __future__ import annotations

import json
import os
import re
import urllib.parse
import urllib.request
from typing import Iterable


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _cfg(config: dict) -> dict:
    raw = config.get("clickhouse") or {}
    if not isinstance(raw, dict):
        return {}
    return raw


def clickhouse_enabled(config: dict) -> bool:
    return bool(_cfg(config).get("enabled", False))


def clickhouse_url(config: dict) -> str:
    return str(
        os.getenv("VETO_LIVE_CLICKHOUSE_URL")
        or _cfg(config).get("url")
        or "http://127.0.0.1:8123"
    ).rstrip("/")


def clickhouse_database(config: dict) -> str:
    return str(
        os.getenv("VETO_LIVE_CLICKHOUSE_DATABASE")
        or _cfg(config).get("database")
        or "veto_live"
    )


def _clean_identifier(value: str) -> str:
    if not IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"Unsafe ClickHouse identifier: {value!r}")
    return f"`{value}`"


def table_name(config: dict, key: str, default: str) -> str:
    raw = str(_cfg(config).get(key) or default)
    parts = raw.split(".")
    if len(parts) == 1:
        parts = [clickhouse_database(config), parts[0]]
    if len(parts) != 2:
        raise ValueError(f"ClickHouse table must be table or database.table: {raw!r}")
    return ".".join(_clean_identifier(part) for part in parts)


def _headers(config: dict) -> dict[str, str]:
    user = os.getenv("VETO_LIVE_CLICKHOUSE_USER") or _cfg(config).get("user")
    password = os.getenv("VETO_LIVE_CLICKHOUSE_PASSWORD") or _cfg(config).get("password")
    headers = {"Content-Type": "text/plain; charset=utf-8"}
    if user:
        headers["X-ClickHouse-User"] = str(user)
    if password:
        headers["X-ClickHouse-Key"] = str(password)
    return headers


def execute(config: dict, query: str, body: bytes | None = None) -> str:
    timeout = float(_cfg(config).get("timeout_seconds") or 30)
    params = urllib.parse.urlencode({"query": query})
    request = urllib.request.Request(
        f"{clickhouse_url(config)}/?{params}",
        data=body,
        headers=_headers(config),
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="replace")


def ping(config: dict) -> bool:
    return execute(config, "SELECT 1").strip() == "1"


def _chunks(rows: list[dict], size: int) -> Iterable[list[dict]]:
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


def insert_json_each_row(config: dict, table: str, rows: list[dict]) -> int:
    if not rows:
        return 0
    batch_size = max(1, int(_cfg(config).get("insert_batch_rows") or 50_000))
    inserted = 0
    for chunk in _chunks(rows, batch_size):
        payload = "\n".join(
            json.dumps(row, ensure_ascii=False, separators=(",", ":"), default=str)
            for row in chunk
        )
        execute(
            config,
            f"INSERT INTO {table} FORMAT JSONEachRow",
            (payload + "\n").encode("utf-8"),
        )
        inserted += len(chunk)
    return inserted


def insert_detail_rows(config: dict, rows: list[dict]) -> int:
    return insert_json_each_row(
        config,
        table_name(config, "detail_table", "live_ts_detail"),
        rows,
    )
