from __future__ import annotations

import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


LIVE_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = LIVE_ROOT.parent
ETL_ROOT = WORKSPACE_ROOT / "ETL"
PROFILE_ROOT = ETL_ROOT / "src" / "profile"

if str(PROFILE_ROOT) not in sys.path:
    sys.path.insert(0, str(PROFILE_ROOT))

from vglive_core import HOST_MAP, PATH_MAP, resolve_channel  # noqa: E402


DEFAULT_CONFIG = LIVE_ROOT / "config" / "live_config.json"
DEFAULT_STATE = LIVE_ROOT / "state" / "live_state.json"
IST_OFFSET_SECONDS = 19_800
SEGMENTS_PER_MINUTE = 10.0


def load_dotenv(path: Path = LIVE_ROOT / ".env") -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and value and key not in os.environ:
            os.environ[key] = value


load_dotenv()


@dataclass(frozen=True)
class SourceConfig:
    name: str
    enabled: bool
    remote_root: str
    local_root: Path
    remote_children: tuple[str, ...] = ()


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(text)
    tmp.replace(path)


def atomic_write_json(path: Path, payload: dict) -> None:
    atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=True))


def load_config(path: Path = DEFAULT_CONFIG) -> dict:
    config = load_json(path, {})
    if not isinstance(config, dict):
        raise SystemExit(f"Config is not a JSON object: {path}")
    return config


def resolve_live_path(value: str | Path) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path
    return (LIVE_ROOT / path).resolve()


def get_sources(config: dict, only_source: str = "all") -> list[SourceConfig]:
    raw_sources = config.get("sources") or {}
    if not isinstance(raw_sources, dict):
        raise SystemExit("config.sources must be an object")

    sources: list[SourceConfig] = []
    for name, item in raw_sources.items():
        if only_source != "all" and name != only_source:
            continue
        if not isinstance(item, dict):
            continue
        enabled = bool(item.get("enabled", True))
        if not enabled:
            continue
        remote_root = str(item.get("remote_root") or "").rstrip("/")
        local_root = resolve_live_path(item.get("local_root") or f"data/raw/source={name}")
        remote_children = tuple(str(child).strip("/") for child in (item.get("remote_children") or []) if str(child).strip("/"))
        if not remote_root:
            raise SystemExit(f"Missing remote_root for source: {name}")
        sources.append(
            SourceConfig(
                name=name,
                enabled=enabled,
                remote_root=remote_root,
                local_root=local_root,
                remote_children=remote_children,
            )
        )
    return sources


def default_rclone_exe() -> str:
    env_exe = os.getenv("VETO_LIVE_RCLONE_EXE")
    if env_exe:
        return str(Path(env_exe).expanduser())
    bundled = ETL_ROOT / "tools" / "rclone" / "rclone.exe"
    return str(bundled) if bundled.exists() else "rclone"


def setup_rclone_env(env: dict[str, str] | None = None) -> dict[str, str]:
    merged = dict(os.environ if env is None else env)
    portable_config = ETL_ROOT / "config" / "rclone.conf"
    if portable_config.exists() and "RCLONE_CONFIG" not in merged:
        merged["RCLONE_CONFIG"] = str(portable_config)
    return merged


def clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_host(value: object) -> str:
    return clean_text(value).lower()


def normalize_status(value: object) -> str:
    raw = clean_text(value)
    if not raw:
        return "Unknown"
    raw = re.sub(r"\.0$", "", raw)
    return raw or "Unknown"


def normalize_ua(value: object) -> str:
    raw = clean_text(value)
    return re.sub(r"\s+", " ", raw)


def safe_key(value: object, default: str = "unknown") -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", clean_text(value).lower()).strip("_")
    return slug or default


def is_ts_path(req_path: object) -> bool:
    return ".ts" in clean_text(req_path).lower()


def channel_candidate_from_path(req_path: object) -> str:
    path = clean_text(req_path).strip()
    lower = path.lower()
    match = re.search(r"(vglive-sk-[0-9]+)", lower)
    if match:
        return match.group(1)

    no_query = lower.split("?", 1)[0].strip("/")
    parts = [part for part in no_query.split("/") if part]
    skip = {"v1", "live", "stream", "hls", "nntv"}
    if parts:
        candidate = parts[1] if parts[0] in skip and len(parts) > 1 else parts[0]
        if candidate:
            return candidate

    filename = parts[-1] if parts else no_query
    filename = re.sub(r"\.ts$", "", filename)
    filename = re.sub(r"[_-]?(1080p?|720p?|480p?|360p?|\d+)$", "", filename)
    return filename.lower()


def platform_for_host(source: str, req_host: object) -> tuple[str, str]:
    host = normalize_host(req_host)
    if source == "fast":
        if "indiatv-samsung" in host or "veto-samsung" in host:
            return "samsung", "Samsung TV Plus - IN"
        if "indiatv-tcl" in host:
            return "tcl", "TCL"
        if "indiatv-cloudtv" in host:
            return "cloudtv", "CloudTV"
        if "indiatv-vi" in host:
            return "vi", "Vi Movies & TV"
        if not host:
            return "unknown", "Unknown FAST Platform"
        label = re.sub(r"\.akamaized\.net$", "", host)
        return safe_key(label), label

    if not host:
        return "unknown", "Unknown Stream Host"
    label = re.sub(r"\.akamaized\.net$", "", host)
    return safe_key(label), label


def resolve_live_channel(req_host: object, req_path: object) -> tuple[str, str]:
    candidate = channel_candidate_from_path(req_path)
    channel = resolve_channel(req_host, candidate)
    return candidate, channel
