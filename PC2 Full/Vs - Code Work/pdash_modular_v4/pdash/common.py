"""
parquet_explorer.py  -  Visual Parquet Explorer (multi-folder + smart browser)
===============================================================================
Run:  streamlit run parquet_explorer.py

pip install streamlit pandas pyarrow
"""

import json
import time
import logging
import urllib.parse
import re
import io
from pathlib import Path
from datetime import time as dtime
from urllib.parse import unquote_plus

import pandas as pd
import pyarrow as pa
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pyarrow.parquet as pq
import pyarrow.compute as pc
import streamlit as st
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle, PageBreak

try:
    import duckdb
    DUCKDB_OK = True
except ImportError:
    DUCKDB_OK = False

try:
    import plotly.express as px
    PLOTLY_OK = True
except ImportError:
    PLOTLY_OK = False

from pdash.config import CONFIG
from pdash.utils.logging_utils import logger, log_warning
WATCH_GAP_CAP_SECONDS = CONFIG.watch_gap_cap_seconds
EXPORT_MEMORY_SAFE_ROW_LIMIT = CONFIG.export_warning_rows

def ub_staged_progress(bar, placeholder, stages: list, fn):
    """
    Streamlit-safe staged progress wrapper.

    Previous versions used a background thread to animate progress while work ran.
    Streamlit is not reliably thread-safe, so this version shows planned stages,
    runs the work in the main Streamlit script context, and preserves exception propagation.
    """
    try:
        for idx, (pct, label) in enumerate(stages):
            bar.progress(int(pct), text=label)
            placeholder.caption(f"⏳ Working — step {idx + 1} of {len(stages)}")
            time.sleep(0.05)
        result = fn()
        bar.progress(100, text="✅ Done!")
        time.sleep(0.2)
        return result
    finally:
        placeholder.empty()
        bar.empty()

# Session defaults shared by app.py
DEFAULTS = {
    "selected_folders": [],
    "last_unique_col":  None,
    "last_unique_vals": [],
    "browser_root":     "",
    "scan_results":     [],
    # Query String Analyzer state
    "qsa_parsed_df":    None,
    "qsa_column":       None,
    "qsa_keys":         [],
    # User Behavior Dashboard state
    "ub_col_map": {
        "queryStr":         "queryStr",
        "reqTimeSec":       "reqTimeSec",
        "reqPath":          "reqPath",
        "UA":               "UA",
        "cliIP":            "cliIP",
        "asn":              "asn",
        "statusCode":       "statusCode",
        "transferTimeMSec": "transferTimeMSec",
        "downloadTime":     "downloadTime",
    },
    "ub_device_ids": [],
    "ub_loaded":     False,
}
