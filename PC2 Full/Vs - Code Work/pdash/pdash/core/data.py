from pdash.common import *

def scan_root(root: str) -> list:
    import os
    root_path = Path(root)
    results   = []

    def _walk(p: Path, depth: int):
        if depth > 4:
            return
        try:
            entries  = list(os.scandir(p))
            pq_count = sum(1 for e in entries
                           if e.is_file(follow_symlinks=False)
                           and e.name.endswith(".parquet"))
            subdirs  = [Path(e.path) for e in entries
                        if e.is_dir(follow_symlinks=False)
                        and not e.name.startswith(".")]
            if pq_count > 0:
                try:
                    rel = str(p.relative_to(root_path))
                except Exception:
                    rel = p.name
                if rel == ".":
                    rel = "(root)"
                results.append({
                    "path":    str(p),
                    "name":    p.name,
                    "n_files": pq_count,
                    "rel":     rel,
                })
            for sub in sorted(subdirs, key=lambda x: x.name):
                _walk(sub, depth + 1)
        except (PermissionError, OSError) as e:
            log_warning(f"Skipping folder during scan: {p}", e)

    _walk(root_path, 0)
    return results


# ─────────────────────────────────────────────
# Core data helpers
# ─────────────────────────────────────────────

@st.cache_data(show_spinner=False, ttl=900)
def collect_files(folders: list) -> list:
    """Return sorted parquet files for selected folders, cached to avoid repeated disk scans."""
    files = []
    for f in folders:
        p = Path(str(f).strip())
        if p.is_dir():
            try:
                files.extend(sorted(p.glob("*.parquet")))
            except Exception as e:
                log_warning(f"Could not list parquet files in {p}", e)
    return files


@st.cache_data(show_spinner=False)
def get_file_columns(folder_key: str) -> dict:
    """Cache per-file schema names so we do not reread parquet schemas repeatedly."""
    files = collect_files(folder_key.split("|"))
    out = {}
    for f in files:
        try:
            out[str(f)] = set(pq.read_schema(f).names)
        except Exception as e:
            log_warning(f"Could not read schema for {f}", e)
            out[str(f)] = set()
    return out

def _parse_query_string(raw_val: str) -> dict:
    try:
        parsed = dict(urllib.parse.parse_qsl(raw_val, keep_blank_values=True))
    except Exception:
        parsed = {}
    parsed["_raw"] = raw_val
    parsed["_count"] = 1
    return parsed


def _iter_query_string_batches(parquet_file: Path, qrystr_col: str, batch_size: int = 50000):
    pf = pq.ParquetFile(parquet_file)
    for batch in pf.iter_batches(columns=[qrystr_col], batch_size=batch_size):
        arr = batch.column(0)
        raws = ["" if x is None else str(x) for x in arr.to_pylist()]
        if not raws:
            continue

def build_mask(tbl, filters: dict):
    mask = None
    for col, vals in filters.items():
        if not vals or col not in tbl.schema.names:
            continue
        col_arr  = tbl.column(col).cast(pa.string())
        sub_mask = None
        for v in vals:
            eq       = pc.equal(col_arr, pa.scalar(str(v), pa.string()))
            sub_mask = eq if sub_mask is None else pc.or_(sub_mask, eq)
        if sub_mask is not None:
            mask = sub_mask if mask is None else pc.and_(mask, sub_mask)
    return mask


@st.cache_data(show_spinner="📋 Reading column schema from parquet files ...")

def load_schema(folder_key: str):
    folders  = folder_key.split("|")
    files    = collect_files(folders)
    all_cols = {}
    seen     = set()
    for f in files:
        fp = str(f.parent)
        if fp in seen:
            continue
        seen.add(fp)
        try:
            schema = pq.read_schema(f)
            for name in schema.names:
                if name not in all_cols:
                    all_cols[name] = str(schema.field(name).type)
        except Exception as e:
            log_warning("Skipped item after recoverable error", e)
            continue
    return list(all_cols.keys()), all_cols


@st.cache_data(show_spinner="🔢 Counting total rows across all files ...")
def count_stats(folder_key: str):
    folders    = folder_key.split("|")
    files      = collect_files(folders)
    total_rows = 0
    for f in files:
        try:
            total_rows += pq.read_metadata(f).num_rows
        except Exception as e:
            log_warning("Recoverable operation failed", e)
    n_folders = len({str(Path(f).parent) for f in files})
    return len(files), total_rows, n_folders



def _dq(identifier: str) -> str:
    """DuckDB-safe double-quoted identifier."""
    return '"' + str(identifier).replace('"', '""') + '"'


@st.cache_data(show_spinner=False, ttl=1800)
def column_completeness_profile(folder_key: str, profile_cols: list, mode: str = "Exact selected columns", sample_rows: int = 100000) -> pd.DataFrame:
    """
    Fast RAM-safe completeness profiling using DuckDB directly over Parquet.

    Empty/null-like means: real NULL, blank string, '-' or '^'.
    Missing columns across files are handled via union_by_name=true and count as NULL.

    Modes:
      - Fast estimate (sample): samples rows and estimates filled/empty % quickly.
      - Exact selected columns: exact scan for selected columns only.
      - Exact all columns: exact scan for all columns passed in.
    """
    profile_cols = list(dict.fromkeys([c for c in profile_cols if c]))
    if not profile_cols:
        return pd.DataFrame(columns=["Column", "Filled %", "Empty %", "Filled rows", "Empty/null-like rows", "Total rows checked", "Mode"])

    files = collect_files(folder_key.split("|"))
    if not files:
        return pd.DataFrame(columns=["Column", "Filled %", "Empty %", "Filled rows", "Empty/null-like rows", "Total rows checked", "Mode"])

    if not DUCKDB_OK:
        raise RuntimeError("DuckDB is required for fast column completeness profiling. Install with: pip install duckdb")

    from pdash.analytics.behavior import ub_get_conn
    con = ub_get_conn()
    parquet_list = [str(f) for f in files]

    sample_sql = ""
    display_mode = mode
    if mode == "Fast estimate (sample)":
        sample_rows = int(sample_rows or 100000)
        sample_sql = f" USING SAMPLE {sample_rows} ROWS"
        display_mode = f"Fast estimate ({sample_rows:,} sampled rows)"

    chunk_size = 40
    results = []
    progress = st.progress(0, text="Profiling columns with DuckDB ...")

    for i in range(0, len(profile_cols), chunk_size):
        chunk_cols = profile_cols[i:i + chunk_size]
        pct = int((i / max(len(profile_cols), 1)) * 100)
        progress.progress(pct, text=f"Profiling columns with DuckDB ... {i:,} / {len(profile_cols):,} columns")

        select_parts = ["COUNT(*) AS total_rows"]
        for idx, col in enumerate(chunk_cols):
            qcol = _dq(col)
            alias = f"empty_{idx}"
            select_parts.append(
                f"SUM(CASE WHEN {qcol} IS NULL OR TRIM(TRY_CAST({qcol} AS VARCHAR)) IN ('', '-', '^') THEN 1 ELSE 0 END) AS {alias}"
            )

        query = f"""
        SELECT
            {', '.join(select_parts)}
        FROM read_parquet({parquet_list!r}, union_by_name=true){sample_sql}
        """

        row = con.execute(query).fetchone()
        if row is None:
            continue

        total = int(row[0] or 0)
        for idx, col in enumerate(chunk_cols):
            empty = int(row[idx + 1] or 0)
            filled = max(total - empty, 0)
            filled_pct = round((filled / total * 100), 2) if total else 0.0
            empty_pct = round((empty / total * 100), 2) if total else 0.0
            results.append({
                "Column": col,
                "Filled %": filled_pct,
                "Empty %": empty_pct,
                "Filled rows": filled,
                "Empty/null-like rows": empty,
                "Total rows checked": total,
                "Mode": display_mode,
            })

    progress.progress(100, text="✅ Column profile complete")
    time.sleep(0.2)
    progress.empty()
    return pd.DataFrame(results)


def unique_values(folder_key: str, column: str) -> pd.DataFrame:
    files = collect_files(folder_key.split("|"))
    file_cols = get_file_columns(folder_key)
    total = len(files)
    counter = {}

    bar = st.progress(0, text=f"Scanning unique values ... 0 / {total:,} files")
    for i, f in enumerate(files):
        pct = int((i + 1) / total * 100) if total else 100
        bar.progress(pct, text=f"Scanning unique values ... {i+1:,} / {total:,} files")
        try:
            if column not in file_cols.get(str(f), set()):
                continue

            pf = pq.ParquetFile(f)
            for batch in pf.iter_batches(columns=[column], batch_size=50000):
                arr = pa.array(batch.column(0)).cast(pa.string())
                vc = pc.value_counts(arr)
                if vc is None:
                    continue
                for item in vc:
                    val = item["values"].as_py()
                    val = str(val) if val is not None else "(null)"
                    counter[val] = counter.get(val, 0) + item["counts"].as_py()
        except Exception as e:
            log_warning("Skipped item after recoverable error", e)
            continue

    bar.empty()
    if not counter:
        return pd.DataFrame(columns=["value", "count", "% of rows"])
    df = pd.DataFrame(list(counter.items()), columns=["value", "count"])
    df = df.sort_values("count", ascending=False).reset_index(drop=True)
    total_rows = df["count"].sum()
    df["% of rows"] = (df["count"] / total_rows * 100).round(2)
    return df


def apply_all_filters(tbl, filters: dict, dual: dict):
    mask = None

    if dual and (dual.get("vals_a") or dual.get("vals_b")):
        col_a, vals_a = dual["col_a"], dual["vals_a"]
        col_b, vals_b = dual["col_b"], dual["vals_b"]
        dm = None
        if vals_a and col_a in tbl.schema.names:
            arr = tbl.column(col_a).cast(pa.string())
            for v in vals_a:
                eq = pc.equal(arr, pa.scalar(str(v), pa.string()))
                dm = eq if dm is None else pc.or_(dm, eq)
        if vals_b and col_b in tbl.schema.names:
            arr = tbl.column(col_b).cast(pa.string())
            for v in vals_b:
                eq = pc.equal(arr, pa.scalar(str(v), pa.string()))
                dm = eq if dm is None else pc.or_(dm, eq)
        if dm is not None:
            mask = dm

    std = build_mask(tbl, filters)
    if std is not None:
        mask = std if mask is None else pc.and_(mask, std)

    if mask is not None:
        tbl = tbl.filter(mask)
    return tbl


def run_query(
    folder_key: str,
    sel_cols: list,
    filters: dict,
    dual: dict,
    max_rows=None,
    progress_label: str = "Scanning files",
) -> pd.DataFrame:
    files = collect_files(folder_key.split("|"))
    file_cols = get_file_columns(folder_key)
    total = len(files)
    frames = []
    collected = 0

    bar = st.progress(0, text=f"{progress_label} ... 0 / {total:,} files")
    info = st.empty()

    needed_cols = list(dict.fromkeys(
        list(sel_cols)
        + list(filters.keys())
        + [dual.get("col_a"), dual.get("col_b")]
    ))
    needed_cols = [c for c in needed_cols if c]

    for i, f in enumerate(files):
        if max_rows is not None and collected >= max_rows:
            break

        pct = int((i + 1) / total * 100) if total else 100
        bar.progress(pct, text=f"{progress_label} ... {i+1:,} / {total:,} files  |  {collected:,} rows found")
        try:
            available_in_file = file_cols.get(str(f), set())
            avail = [c for c in needed_cols if c in available_in_file]
            if not any(c in available_in_file for c in sel_cols):
                continue
            if not avail:
                continue

            pf = pq.ParquetFile(f)
            for batch in pf.iter_batches(columns=avail, batch_size=50000):
                tbl = pa.Table.from_batches([batch])
                tbl = apply_all_filters(tbl, filters, dual)
                if len(tbl) == 0:
                    continue

                present_sel_cols = [c for c in sel_cols if c in tbl.schema.names]
                if not present_sel_cols:
                    continue
                tbl = tbl.select(present_sel_cols)

                need = (max_rows - collected) if max_rows is not None else len(tbl)
                if need <= 0:
                    break

                chunk = tbl.slice(0, need).to_pandas()
                chunk.insert(0, "_folder", f.parent.name)
                frames.append(chunk)
                collected += len(chunk)

                if max_rows is not None and collected >= max_rows:
                    break
        except Exception as e:
            log_warning("Skipped item after recoverable error", e)
            continue

    bar.empty()
    info.empty()
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def full_filtered_value_counts(
    folder_key: str,
    target_col: str,
    filters: dict,
    dual: dict,
    top_n: int = 20,
    include_other: bool = True,
    progress_label: str = "Building full-dataset chart",
) -> pd.DataFrame:
    """Exact value-count aggregation over the FULL filtered parquet dataset.

    This does NOT materialize matching rows into pandas. It streams only the
    target column plus filter columns from parquet, applies the same filters,
    and aggregates counts batch-by-batch with PyArrow.
    """
    files = collect_files(folder_key.split("|"))
    file_cols = get_file_columns(folder_key)
    total = len(files)
    counter = {}

    needed_cols = list(dict.fromkeys(
        [target_col]
        + list(filters.keys())
        + [dual.get("col_a"), dual.get("col_b")]
    ))
    needed_cols = [c for c in needed_cols if c]

    bar = st.progress(0, text=f"{progress_label} ... 0 / {total:,} files")
    for i, f in enumerate(files):
        pct = int((i + 1) / total * 100) if total else 100
        bar.progress(pct, text=f"{progress_label} ... {i+1:,} / {total:,} files")
        try:
            available = file_cols.get(str(f), set())
            if target_col not in available:
                continue
            avail = [c for c in needed_cols if c in available]
            if target_col not in avail:
                continue

            pf = pq.ParquetFile(f)
            for batch in pf.iter_batches(columns=avail, batch_size=100000):
                tbl = pa.Table.from_batches([batch])
                tbl = apply_all_filters(tbl, filters, dual)
                if len(tbl) == 0 or target_col not in tbl.schema.names:
                    continue

                arr = tbl.column(target_col).cast(pa.string())
                vc = pc.value_counts(arr)
                if vc is None:
                    continue
                for item in vc:
                    val = item["values"].as_py()
                    val = str(val) if val is not None else "(null)"
                    if val == "":
                        val = "(blank)"
                    counter[val] = counter.get(val, 0) + int(item["counts"].as_py())
        except Exception as e:
            log_warning("Skipped item after recoverable error", e)
            continue

    bar.empty()
    if not counter:
        return pd.DataFrame(columns=[target_col, "count"])

    full_df = pd.DataFrame(counter.items(), columns=[target_col, "count"]).sort_values("count", ascending=False)
    if include_other and len(full_df) > int(top_n):
        head = full_df.head(int(top_n)).copy()
        other_count = int(full_df.iloc[int(top_n):]["count"].sum())
        head = pd.concat([head, pd.DataFrame([{target_col: "Other", "count": other_count}])], ignore_index=True)
        out = head
    else:
        out = full_df.head(int(top_n)).copy()
    total_count = int(full_df["count"].sum())
    out["% of filtered rows"] = (out["count"] / total_count * 100).round(2) if total_count else 0
    return out.reset_index(drop=True)



# ─────────────────────────────────────────────
# Query String helpers  (NEW)

