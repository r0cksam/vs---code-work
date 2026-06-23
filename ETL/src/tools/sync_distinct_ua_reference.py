#!/usr/bin/env python3
"""Keep the reusable distinct-UA reference in sync with incremental profiles."""

from __future__ import annotations

import argparse
import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


ETL_ROOT = Path(__file__).resolve().parents[2]
DECODER_PATH = ETL_ROOT / "src" / "tools" / "decode_distinct_ua_lookup.py"
DEFAULT_REFERENCE = ETL_ROOT / "distinct_UA_Both_All.csv"
DEFAULT_PARQUET = ETL_ROOT / "output" / "device_decode" / "distinct_UA_Both_All.parquet"
DEFAULT_MANIFEST = ETL_ROOT / "output" / "device_decode" / "distinct_UA_Both_All_manifest.json"


def load_decoder() -> Any:
    spec = importlib.util.spec_from_file_location("decode_distinct_ua_lookup", DECODER_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load decoder module from {DECODER_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


decoder = load_decoder()


def read_reference(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["UA"])
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    selected = "UA" if "UA" in df.columns else df.columns[0]
    return pd.DataFrame({"UA": df[selected].map(decoder.safe_text)})


def read_profile(path: Path, column: str | None) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"UA profile not found: {path}")
    df = pd.read_parquet(path)
    if df.empty:
        return pd.DataFrame(columns=["UA"])
    selected = column
    if not selected:
        for candidate in ["ua_sample", "ua_norm_key", "UA", "ua_norm", "userAgent"]:
            if candidate in df.columns:
                selected = candidate
                break
    if not selected or selected not in df.columns:
        raise SystemExit(f"No UA column found in {path}. Available columns: {', '.join(df.columns)}")
    return pd.DataFrame({"UA": df[selected].map(decoder.safe_text)})


def normalize(frame: pd.DataFrame, origin: str) -> pd.DataFrame:
    out = frame.copy()
    out["UA"] = out["UA"].map(decoder.safe_text)
    out["ua_norm"] = out["UA"].map(decoder.normalize_ua)
    out = out[out["ua_norm"] != ""].copy()
    out["ua_hash"] = out["ua_norm"].map(decoder.ua_hash)
    out["origin"] = origin
    return out


def atomic_write_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.stem}.tmp{path.suffix}")
    frame.to_csv(tmp, index=False, encoding="utf-8")
    tmp.replace(path)


def atomic_write_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.stem}.tmp{path.suffix}")
    frame.to_parquet(tmp, index=False, compression="zstd")
    tmp.replace(path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Union incremental UA profile rows into the reusable distinct-UA reference.")
    parser.add_argument("--reference", type=Path, default=DEFAULT_REFERENCE)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--profile-column", default=None)
    parser.add_argument("--out-parquet", type=Path, default=DEFAULT_PARQUET)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    args.reference = args.reference.expanduser().resolve()
    args.profile = args.profile.expanduser().resolve()
    args.out_parquet = args.out_parquet.expanduser().resolve()
    args.manifest = args.manifest.expanduser().resolve()

    existing = normalize(read_reference(args.reference), "existing")
    incoming = normalize(read_profile(args.profile, args.profile_column), "incremental_profile")
    before = len(existing)

    combined = pd.concat([existing, incoming], ignore_index=True)
    combined = combined.drop_duplicates("ua_hash", keep="first").sort_values("UA").reset_index(drop=True)
    reference_out = combined[["UA"]]
    parquet_out = combined[["UA", "ua_norm", "ua_hash", "origin"]]

    added = int(len(combined) - before)
    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "reference": str(args.reference),
        "profile": str(args.profile),
        "out_parquet": str(args.out_parquet),
        "existing_distinct_ua": int(before),
        "profile_distinct_ua": int(len(incoming.drop_duplicates("ua_hash"))),
        "final_distinct_ua": int(len(combined)),
        "added_distinct_ua": added,
        "dry_run": bool(args.dry_run),
    }

    if not args.dry_run:
        atomic_write_csv(reference_out, args.reference)
        atomic_write_parquet(parquet_out, args.out_parquet)
        args.manifest.parent.mkdir(parents=True, exist_ok=True)
        tmp_manifest = args.manifest.with_name(f"{args.manifest.stem}.tmp{args.manifest.suffix}")
        tmp_manifest.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        tmp_manifest.replace(args.manifest)

    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
