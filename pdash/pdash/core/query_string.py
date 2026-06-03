from pdash.common import *
from pdash.core.data import collect_files, get_file_columns, _iter_query_string_batches

def parse_qrystr_column(df: pd.DataFrame, value_col: str, count_col: str = "count") -> pd.DataFrame:
    """
    Given a DataFrame where each row is a unique query-string value + its count,
    parse every query string and return a flat DataFrame with one row per unique value,
    preserving the count column.
    """
    records = []
    for _, row in df.iterrows():
        raw_val = str(row[value_col])
        cnt     = row.get(count_col, 1)
        try:
            parsed = dict(urllib.parse.parse_qsl(raw_val, keep_blank_values=True))
        except Exception:
            parsed = {}
        parsed["_raw"]   = raw_val
        parsed["_count"] = cnt
        records.append(parsed)
    return pd.DataFrame(records).fillna("")


def load_qrystr_from_parquet(
    folder_key: str,
    qrystr_col: str,
    progress_label: str = "Loading query strings",
) -> pd.DataFrame:
    """
    Read the qrystr column from parquet files, return a flat parsed DataFrame.
    Each parquet row becomes one parsed record; _count=1 for raw parquet rows.
    """
    files = collect_files(folder_key.split("|"))
    file_cols = get_file_columns(folder_key)
    total = len(files)
    frames = []

    bar = st.progress(0, text=f"{progress_label} ... 0 / {total:,} files")
    for i, f in enumerate(files):
        pct = int((i + 1) / total * 100) if total else 100
        bar.progress(pct, text=f"{progress_label} ... {i+1:,} / {total:,} files")
        try:
            if qrystr_col not in file_cols.get(str(f), set()):
                continue
            for parsed_batch_df in _iter_query_string_batches(f, qrystr_col, batch_size=50000):
                frames.append(parsed_batch_df)
        except Exception as e:
            log_warning("Skipped item after recoverable error", e)
            continue
    bar.empty()
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)



# ─────────────────────────────────────────────
# User Behavior Dashboard helpers
