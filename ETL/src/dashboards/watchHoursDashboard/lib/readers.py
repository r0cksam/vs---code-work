"""
lib/readers.py — Profile CSV/Parquet readers, lake partition helpers, ASN enrichment.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from .constants import (
    CHUNK_DURATION_HOURS,
    DAILY_TABLE_NAMES,
    IST,
    PROFILE_FILES,
)
from .utils import (
    decode_cols,
    normalize_asn,
    one_row,
    records,
    total,
    with_numbers,
)

try:
    from vglive_core import DEFAULT_LAKE_FOLDER
except Exception:
    from os import getenv

    DEFAULT_LAKE_FOLDER = Path(
        getenv(
            "VG_ETL_LAKE_ROOT",
            str(Path(__file__).resolve().parents[4] / "data" / "lake"),
        )
    )


# ── Low-level readers ─────────────────────────────────────────────────────────

OPTIONAL_DAILY_TABLES = {
    "channel_geo_daily",
    "region_channel_audience_daily",
    "region_channel_device_daily",
}


def read_csv(profile_dir: Path, key: str) -> pd.DataFrame:
    """Read a profile CSV (or its Parquet equivalent if available)."""
    path = profile_dir / PROFILE_FILES[key]
    parquet_path = path.with_suffix(".parquet")
    if parquet_path.exists():
        return pd.read_parquet(parquet_path)
    if path.exists():
        return pd.read_csv(path)
    return pd.DataFrame()


def read_store_table(profile_dir: Path, table_name: str) -> pd.DataFrame:
    """Read a daily aggregation table used by dashboard date filters."""
    path = profile_dir.parent / "daily_tables" / f"{table_name}.parquet"
    if path.exists():
        return pd.read_parquet(path)
    csv_path = path.with_suffix(".csv")
    if csv_path.exists():
        return pd.read_csv(csv_path)
    return pd.DataFrame()


def read_all_daily_tables(profile_dir: Path) -> dict[str, list[dict]]:
    """Read every daily table listed in DAILY_TABLE_NAMES and return as records."""
    return {
        name: records(read_store_table(profile_dir, name))
        for name in DAILY_TABLE_NAMES
    }


def missing_profile_inputs(profile_dir: Path) -> list[str]:
    """Return required profile/daily table files that are missing."""
    required_profile_keys = ["channel_summary", "channel_daily", "daily", "status", "files"]
    missing: list[str] = []
    for key in required_profile_keys:
        base = profile_dir / PROFILE_FILES[key]
        if not base.exists() and not base.with_suffix(".parquet").exists():
            missing.append(str(base.name))

    daily_dir = profile_dir.parent / "daily_tables"
    for name in DAILY_TABLE_NAMES:
        if name in OPTIONAL_DAILY_TABLES:
            continue
        parquet_path = daily_dir / f"{name}.parquet"
        csv_path = daily_dir / f"{name}.csv"
        if not parquet_path.exists() and not csv_path.exists():
            missing.append(str(parquet_path.relative_to(profile_dir.parent)))
    return missing


# ── Lake partition helpers ────────────────────────────────────────────────────

def lake_partition_files(lake: Path, date_text: str) -> list[Path]:
    """Return Parquet files for a given date from Hive-partitioned lake folders."""
    try:
        dt = datetime.strptime(date_text, "%Y-%m-%d")
    except ValueError:
        return []
    candidates = [
        lake / f"year={dt.year:04d}" / f"month={dt.month:02d}" / f"day={dt.day:02d}",
        lake / f"{dt.year:04d}" / f"{dt.month:02d}" / f"{dt.day:02d}",
    ]
    candidates.extend(
        lake.glob(f"source=*/year={dt.year:04d}/month={dt.month:02d}/day={dt.day:02d}")
    )
    files: list[Path] = []
    for folder in candidates:
        if folder.exists():
            files.extend(sorted(folder.glob("*.parquet")))
    return files


def resolve_inventory_file(path_text: object, lake: Path = DEFAULT_LAKE_FOLDER) -> Path:
    """Resolve an inventory file path, handling both absolute and lake-relative formats."""
    path = Path(str(path_text))
    if path.exists():
        return path
    normalized = str(path_text).replace("\\", "/")
    source_marker = "/source="
    if source_marker in normalized:
        rel = normalized.split(source_marker, 1)[1]
        return lake / ("source=" + rel.replace("/", "\\"))
    year_marker = "/year="
    if year_marker in normalized:
        rel = normalized.split(year_marker, 1)[1]
        return lake / ("year=" + rel.replace("/", "\\"))
    return path


def inventory_files_for_date(files: pd.DataFrame, date_text: str) -> list[Path]:
    """Return resolved file paths for a given date from the file inventory DataFrame."""
    if files.empty or "date" not in files.columns or "file" not in files.columns:
        return []
    day_files = files[files["date"].astype(str) == date_text]["file"].tolist()
    return [resolve_inventory_file(p) for p in day_files]


# ── True data range ───────────────────────────────────────────────────────────

def _min_max_req_epoch(files: list[Path]) -> tuple[float | None, float | None]:
    """Scan Parquet files for min/max reqTimeSec values."""
    if not files:
        return None, None
    try:
        import pyarrow.parquet as pq
    except Exception:
        return None, None

    lo: float | None = None
    hi: float | None = None
    for file in files:
        try:
            pf = pq.ParquetFile(file)
            if "reqTimeSec" not in pf.schema_arrow.names:
                continue
            for batch in pf.iter_batches(columns=["reqTimeSec"], batch_size=1_000_000):
                values = pd.to_numeric(batch.column(0).to_pandas(), errors="coerce").dropna()
                if values.empty:
                    continue
                b_min, b_max = float(values.min()), float(values.max())
                lo = b_min if lo is None else min(lo, b_min)
                hi = b_max if hi is None else max(hi, b_max)
        except Exception:
            continue
    return lo, hi


def _format_epoch_ist(epoch: float | None) -> str:
    if epoch is None:
        return ""
    return datetime.fromtimestamp(epoch, IST).strftime("%Y-%m-%d %H:%M:%S")


def true_data_range(
    file_dates: list[str],
    files: pd.DataFrame,
    lake: Path = DEFAULT_LAKE_FOLDER,
) -> dict[str, str]:
    """Return the true first/last timestamp of data across the date range."""
    if not file_dates:
        return {"first": "", "last": ""}
    first_files = inventory_files_for_date(files, file_dates[0]) or lake_partition_files(lake, file_dates[0])
    last_files  = inventory_files_for_date(files, file_dates[-1]) or lake_partition_files(lake, file_dates[-1])
    first_min, _ = _min_max_req_epoch(first_files)
    _, last_max   = _min_max_req_epoch(last_files)
    return {
        "first": _format_epoch_ist(first_min) or f"{file_dates[0]} 00:00:00",
        "last":  _format_epoch_ist(last_max)  or f"{file_dates[-1]} 23:59:59",
    }


# ── ASN enrichment ────────────────────────────────────────────────────────────

def load_asn_decoded(path: Path) -> pd.DataFrame:
    """Load and normalise the pre-decoded ASN lookup table."""
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    if "asn" not in df.columns:
        return pd.DataFrame()
    df = df.copy()
    df["asn"] = df["asn"].map(normalize_asn)
    keep = [c for c in ["asn", "as_name", "as_country", "as_domain", "asn_type", "lookup_status"] if c in df.columns]
    return df[keep].drop_duplicates("asn")


def enrich_asn(asn_df: pd.DataFrame, decoded_df: pd.DataFrame) -> pd.DataFrame:
    """Merge ASN rows with the decoded lookup table and add display columns."""
    if asn_df.empty:
        return asn_df
    out = asn_df.copy()
    out["asn"] = out["asn"].map(normalize_asn)
    if not decoded_df.empty:
        out = out.merge(decoded_df, on="asn", how="left")
    for col in ["as_name", "as_country", "as_domain", "asn_type", "lookup_status"]:
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].fillna("")
    out["as_name"] = out.apply(
        lambda row: row["as_name"] if row["as_name"] else f"AS{row['asn']}",
        axis=1,
    )
    out["asn_display"] = out.apply(lambda row: f"AS{row['asn']} - {row['as_name']}", axis=1)
    return out
