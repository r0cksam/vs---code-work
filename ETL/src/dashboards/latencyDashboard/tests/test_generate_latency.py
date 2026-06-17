# FIX-M3
import argparse  # FIX-M3
import importlib.util  # FIX-M3
from pathlib import Path  # FIX-M3
from unittest.mock import Mock, patch  # FIX-M3
import pytest  # FIX-M3
MODULE_PATH = Path(__file__).resolve().parents[1] / "generate_latency.py"  # FIX-M3
SPEC = importlib.util.spec_from_file_location("generate_latency_under_test", MODULE_PATH)  # FIX-M3
assert SPEC is not None  # FIX-M3
assert SPEC.loader is not None  # FIX-M3
generate_latency = importlib.util.module_from_spec(SPEC)  # FIX-M3
SPEC.loader.exec_module(generate_latency)  # FIX-M3
def test_validate_lake_path_valid_path_inside_base_passes(tmp_path, monkeypatch):  # FIX-M3
    base = tmp_path / "base"  # FIX-M3
    lake = base / "lake"  # FIX-M3
    lake.mkdir(parents=True)  # FIX-M3
    monkeypatch.setenv("VG_LAKE_BASE_ROOT", str(base))  # FIX-M3
    generate_latency.validate_lake_path(lake)  # FIX-M3
def test_validate_lake_path_outside_base_raises_value_error(tmp_path, monkeypatch):  # FIX-M3
    base = tmp_path / "base"  # FIX-M3
    lake = tmp_path / "outside" / "lake"  # FIX-M3
    base.mkdir()  # FIX-M3
    lake.mkdir(parents=True)  # FIX-M3
    monkeypatch.setenv("VG_LAKE_BASE_ROOT", str(base))  # FIX-M3
    with pytest.raises(ValueError):  # FIX-M3
        generate_latency.validate_lake_path(lake)  # FIX-M3
def test_checked_dates_mismatched_start_end_raises_value_error():  # FIX-M3
    args = argparse.Namespace(start="2026-06-01", end=None, window_days=0, lake=Path("."))  # FIX-M3
    with pytest.raises(ValueError):  # FIX-M3
        generate_latency.checked_dates(args)  # FIX-M3
def test_fetch_df_duckdb_error_reraises_runtime_error():  # FIX-M3
    con = Mock()  # FIX-M3
    con.execute.side_effect = generate_latency.duckdb.Error("boom")  # FIX-M3
    with pytest.raises(RuntimeError):  # FIX-M3
        generate_latency.fetch_df(con, "SELECT broken")  # FIX-M3
def test_load_tables_from_profile_missing_summary_raises_file_not_found(tmp_path):  # FIX-M3
    profile_dir = tmp_path / "profile"  # FIX-M3
    profile_dir.mkdir()  # FIX-M3
    with patch.object(generate_latency, "read_profile_table", return_value=object()):  # FIX-M3
        with pytest.raises(FileNotFoundError):  # FIX-M3
            generate_latency.load_tables_from_profile(profile_dir)  # FIX-M3
