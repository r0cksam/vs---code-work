"""
parquet_explorer.py  -  Visual Parquet Explorer (multi-folder + smart browser)
===============================================================================
Run:  streamlit run parquet_explorer.py

pip install streamlit pandas pyarrow
"""

import json
import time
import urllib.parse
import re
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import streamlit as st


# ─────────────────────────────────────────────
# Page config
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Parquet Explorer",
    page_icon="🗂️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .block-container { padding-top: 1.5rem; }
    .stMetric label  { font-size: 0.78rem; color: #888; }
    div[data-testid="stSidebarContent"] { padding-top: 1rem; }
    div[data-testid="stSidebarContent"] .stButton button {
        width: 100%;
        text-align: left;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────
# Session state
# ─────────────────────────────────────────────
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
}
for k, v in DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v


# ─────────────────────────────────────────────
# Folder scanner
# ─────────────────────────────────────────────

@st.cache_data(show_spinner=False)
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
        except (PermissionError, OSError):
            pass

    _walk(root_path, 0)
    return results


# ─────────────────────────────────────────────
# Core data helpers
# ─────────────────────────────────────────────

def collect_files(folders: list) -> list:
    files = []
    for f in folders:
        p = Path(str(f).strip())
        if p.is_dir():
            files.extend(sorted(p.glob("*.parquet")))
    return files


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


@st.cache_data(show_spinner="Reading schema ...")
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
        except Exception:
            continue
    return list(all_cols.keys()), all_cols


@st.cache_data(show_spinner="Counting rows ...")
def count_stats(folder_key: str):
    folders    = folder_key.split("|")
    files      = collect_files(folders)
    total_rows = 0
    for f in files:
        try:
            total_rows += pq.read_metadata(f).num_rows
        except Exception:
            pass
    n_folders = len({str(Path(f).parent) for f in files})
    return len(files), total_rows, n_folders


def unique_values(folder_key: str, column: str) -> pd.DataFrame:
    folders = folder_key.split("|")
    files   = collect_files(folders)
    total   = len(files)
    counter = {}

    bar = st.progress(0, text=f"Scanning unique values ... 0 / {total:,} files")
    for i, f in enumerate(files):
        pct = int((i + 1) / total * 100) if total else 100
        bar.progress(pct, text=f"Scanning unique values ... {i+1:,} / {total:,} files")
        try:
            if column not in pq.read_schema(f).names:
                continue
            tbl = pq.read_table(f, columns=[column])
            for item in tbl.column(column).value_counts():
                val = item["values"].as_py()
                val = str(val) if val is not None else "(null)"
                counter[val] = counter.get(val, 0) + item["counts"].as_py()
        except Exception:
            continue

    bar.empty()
    if not counter:
        return pd.DataFrame(columns=["value", "count", "% of rows"])
    df    = pd.DataFrame(list(counter.items()), columns=["value", "count"])
    df    = df.sort_values("count", ascending=False).reset_index(drop=True)
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
    folders   = folder_key.split("|")
    files     = collect_files(folders)
    total     = len(files)
    frames    = []
    collected = 0

    bar  = st.progress(0, text=f"{progress_label} ... 0 / {total:,} files")
    info = st.empty()

    for i, f in enumerate(files):
        if max_rows is not None and collected >= max_rows:
            break
        pct  = int((i + 1) / total * 100) if total else 100
        bar.progress(pct, text=f"{progress_label} ... {i+1:,} / {total:,} files  |  {collected:,} rows found")
        try:
            avail = [c for c in sel_cols if c in pq.read_schema(f).names]
            if not avail:
                continue
            tbl   = pq.read_table(f, columns=avail)
            tbl   = apply_all_filters(tbl, filters, dual)
            need  = (max_rows - collected) if max_rows else len(tbl)
            chunk = tbl.slice(0, need).to_pandas()
            chunk.insert(0, "_folder", f.parent.name)
            frames.append(chunk)
            collected += len(chunk)
        except Exception:
            continue

    bar.empty()
    info.empty()
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ─────────────────────────────────────────────
# Query String helpers  (NEW)
# ─────────────────────────────────────────────

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
    folders = folder_key.split("|")
    files   = collect_files(folders)
    total   = len(files)
    frames  = []

    bar = st.progress(0, text=f"{progress_label} ... 0 / {total:,} files")
    for i, f in enumerate(files):
        pct = int((i + 1) / total * 100) if total else 100
        bar.progress(pct, text=f"{progress_label} ... {i+1:,} / {total:,} files")
        try:
            schema_names = pq.read_schema(f).names
            if qrystr_col not in schema_names:
                continue
            tbl = pq.read_table(f, columns=[qrystr_col])
            pdf = tbl.to_pandas()
            pdf.rename(columns={qrystr_col: "_raw"}, inplace=True)
            pdf["_count"] = 1
            records = []
            for _, row in pdf.iterrows():
                try:
                    parsed = dict(urllib.parse.parse_qsl(str(row["_raw"]), keep_blank_values=True))
                except Exception:
                    parsed = {}
                parsed["_raw"]   = row["_raw"]
                parsed["_count"] = 1
                records.append(parsed)
            frames.append(pd.DataFrame(records).fillna(""))
        except Exception:
            continue
    bar.empty()
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────

with st.sidebar:
    st.title("🗂️ Parquet Explorer")
    st.markdown("---")

    mode = st.radio(
        "Folder selection mode",
        ["🔍 Browse from root", "✏️ Type paths manually"],
        horizontal=False,
        key="folder_mode",
    )

    st.markdown("---")

    if mode == "🔍 Browse from root":
        st.markdown("**Step 1 — Enter a root folder**")
        st.caption("The app will scan it and show every subfolder that has .parquet files.")

        root_input = st.text_input(
            "Root folder",
            value=st.session_state.browser_root,
            placeholder=r"e.g. D:\data  or  /mnt/data",
            key="root_input",
        )

        root_changed = root_input.strip() != st.session_state.browser_root

        if st.button("🔍 Scan", type="primary", key="btn_scan") or (
            root_input.strip() and root_changed
        ):
            rp = Path(root_input.strip())
            if not rp.is_dir():
                st.error("Folder not found.")
            else:
                st.session_state.browser_root = root_input.strip()
                with st.spinner("Scanning ..."):
                    st.session_state.scan_results = scan_root(root_input.strip())
                if not st.session_state.scan_results:
                    st.warning("No subfolders with .parquet files found.")

        results = st.session_state.scan_results

        if results:
            st.markdown(f"**Step 2 — Pick folders** ({len(results)} found)")
            st.caption("Click to toggle. Selected folders are merged for analysis.")

            ca, cb = st.columns(2)
            with ca:
                if st.button("✅ Select all", key="sel_all"):
                    st.session_state.selected_folders = [r["path"] for r in results]
                    st.rerun()
            with cb:
                if st.button("🗑️ Clear all", key="clr_all"):
                    st.session_state.selected_folders = []
                    st.rerun()

            selected_set = set(st.session_state.selected_folders)

            for r in results:
                is_selected = r["path"] in selected_set
                icon  = "✅" if is_selected else "⬜"
                label = f"{icon}  {r['rel']}  ({r['n_files']:,} files)"
                if st.button(label, key=f"fld_{r['path']}"):
                    if is_selected:
                        st.session_state.selected_folders = [
                            x for x in st.session_state.selected_folders if x != r["path"]
                        ]
                    else:
                        if r["path"] not in st.session_state.selected_folders:
                            st.session_state.selected_folders.append(r["path"])
                    st.rerun()

            if st.session_state.selected_folders:
                st.markdown("---")
                st.success(f"{len(st.session_state.selected_folders)} folder(s) selected")

    else:
        st.markdown("**Type folder paths manually**")
        st.caption("One path per box. All must contain .parquet files.")

        if "manual_folders" not in st.session_state:
            st.session_state.manual_folders = [""]

        manual = st.session_state.manual_folders
        for i in range(len(manual)):
            manual[i] = st.text_input(
                f"Folder {i+1}",
                value=manual[i],
                placeholder=r"e.g. D:\data\parquet_jan",
                key=f"manual_{i}",
            )

        mc1, mc2 = st.columns(2)
        with mc1:
            if st.button("➕ Add", key="man_add"):
                st.session_state.manual_folders.append("")
                st.rerun()
        with mc2:
            if st.button("➖ Remove", key="man_rem") and len(manual) > 1:
                st.session_state.manual_folders.pop()
                st.rerun()

        valid = []
        for f in manual:
            f = f.strip()
            if not f:
                continue
            p = Path(f)
            if not p.is_dir():
                st.error(f"Not found: {f}")
            elif not list(p.glob("*.parquet")):
                st.warning(f"No .parquet files: {f}")
            else:
                valid.append(f)
        st.session_state.selected_folders = valid

    st.markdown("---")
    st.caption("Tabs:\n1. Columns\n2. Unique Values\n3. Filter & Export\n4. Query String Analyzer")


# ─────────────────────────────────────────────
# Resolve valid folders
# ─────────────────────────────────────────────

valid_folders = []
for f in st.session_state.selected_folders:
    f = str(f).strip()
    p = Path(f)
    if p.is_dir() and list(p.glob("*.parquet")):
        valid_folders.append(f)


# ─────────────────────────────────────────────
# Welcome screen
# ─────────────────────────────────────────────
if not valid_folders:
    st.title("Welcome to Parquet Explorer")
    st.info("👈  Use the sidebar to pick your Parquet folders.")
    st.markdown("""
    **Two ways to add folders:**
    - **Browse from root** — type a root path, click Scan, then click any folder to select it
    - **Type paths manually** — paste exact folder paths one by one

    **Features:**
    - 📋 Browse all columns and data types
    - 🔍 Count + explore unique values per column
    - 📥 Export unique values list as CSV
    - 🎯 Filter rows by multiple columns (AND logic)
    - 🔗 Dual-column filter (OR between two columns)
    - 👀 Preview matching rows
    - 💾 Export filtered data as CSV
    - 📂 Multiple folders merged transparently
    - 🔎 **Query String Analyzer** — parse URL query strings, track device IDs & session IDs
    """)
    st.stop()


# ─────────────────────────────────────────────
# Header
# ─────────────────────────────────────────────

folder_key             = "|".join(sorted(valid_folders))
columns, col_types     = load_schema(folder_key)
n_files, n_rows, n_fol = count_stats(folder_key)

if not columns:
    st.error("Could not read any schema from the selected folders.")
    st.stop()

st.title("🗂️ Parquet Explorer")

m1, m2, m3, m4 = st.columns(4)
m1.metric("Folders",       f"{n_fol}")
m2.metric("Parquet files", f"{n_files:,}")
m3.metric("Total rows",    f"{n_rows:,}")
m4.metric("Columns",       f"{len(columns)}")

with st.expander(f"📂 {n_fol} active folder(s)", expanded=False):
    for f in valid_folders:
        fpath = Path(f)
        cnt   = len(list(fpath.glob("*.parquet")))
        st.markdown(f"- **{fpath.name}** &nbsp; `{f}` &nbsp; ({cnt:,} files)")

st.markdown("---")

tab1, tab2, tab3, tab4 = st.tabs([
    "📋 Columns",
    "🔍 Unique Values",
    "🎯 Filter & Export",
    "🔎 Query String Analyzer",
])


# ══════════════════════════════════════════════
# TAB 1 — Column browser
# ══════════════════════════════════════════════
with tab1:
    st.subheader("All Columns")
    st.caption("Union of all columns across selected folders.")

    search  = st.text_input("Search column name", placeholder="type to filter ...", key="col_search")
    cols_df = pd.DataFrame({
        "Column":    columns,
        "Data type": [col_types.get(c, "") for c in columns],
    })
    if search:
        cols_df = cols_df[cols_df["Column"].str.contains(search, case=False)]

    st.dataframe(cols_df, use_container_width=True, height=520, hide_index=True)


# ══════════════════════════════════════════════
# TAB 2 — Unique values
# ══════════════════════════════════════════════
with tab2:
    st.subheader("Unique Values Explorer")
    st.caption("Pick a column, count, then explore or export.")

    col_pick = st.selectbox("Column", options=columns, key="uv_col")

    if st.button("1  Count unique values", type="secondary", key="btn_count"):
        udf_all = unique_values(folder_key, col_pick)
        st.session_state[f"udf_{col_pick}"] = udf_all
        st.session_state[f"n_{col_pick}"]   = len(udf_all)

    total_unique = st.session_state.get(f"n_{col_pick}")
    udf_cached   = st.session_state.get(f"udf_{col_pick}")

    if total_unique is not None:
        st.success(f"**{total_unique:,}** unique values in `{col_pick}`")
        st.markdown("---")

        c_inp, c_show, c_exp = st.columns([2, 1, 1])
        with c_inp:
            show_n = st.number_input(
                f"How many to show (max {total_unique:,})",
                min_value=1,
                max_value=total_unique,
                value=min(total_unique, 200),
                step=1,
                key="uv_show_n",
            )
        with c_show:
            st.markdown("<br>", unsafe_allow_html=True)
            load_btn = st.button("2  Show values", type="primary", key="btn_show")
        with c_exp:
            st.markdown("<br>", unsafe_allow_html=True)
            st.download_button(
                label="Export all CSV",
                data=udf_cached.to_csv(index=False).encode(),
                file_name=f"unique_{col_pick}.csv",
                mime="text/csv",
                key="btn_exp_unique",
            )

        if load_btn:
            st.session_state["uv_result"]    = udf_cached.head(int(show_n))
            st.session_state.last_unique_col  = col_pick
            st.session_state.last_unique_vals = st.session_state["uv_result"]["value"].tolist()

        udf_result = st.session_state.get("uv_result")
        if udf_result is not None and not udf_result.empty:
            st.caption(f"Showing top {len(udf_result):,} of {total_unique:,} unique values")

            uv_search  = st.text_input("Search within values", placeholder="filter the list ...", key="uv_search")
            display_df = udf_result
            if uv_search:
                display_df = udf_result[
                    udf_result["value"].astype(str).str.contains(uv_search, case=False, na=False)
                ]
                st.caption(f"{len(display_df):,} values match your search")

            col_l, col_r = st.columns([2, 1])
            with col_l:
                st.dataframe(display_df, use_container_width=True, height=420, hide_index=True)
            with col_r:
                top10 = display_df.head(10)
                if not top10.empty:
                    st.bar_chart(top10.set_index("value")["count"])

            st.markdown("---")
            st.markdown("#### Extract all rows for a specific value")
            st.caption(
                f"Pick one or more values from `{col_pick}` — get every matching row across all files as a CSV."
            )

            pick_vals = st.multiselect(
                f"Select value(s) from `{col_pick}`",
                options=display_df["value"].tolist(),
                placeholder="Click or type to pick values ...",
                key="extract_vals",
            )

            extract_cols = st.multiselect(
                "Columns to include in extracted CSV  (leave empty = all columns)",
                options=columns,
                default=[],
                key="extract_cols",
            )

            if pick_vals:
                st.info(
                    f"Will extract all rows where `{col_pick}` is one of: "
                    + ", ".join(f"**{v}**" for v in pick_vals[:5])
                    + (f" ... (+{len(pick_vals)-5} more)" if len(pick_vals) > 5 else "")
                )

                xc1, xc2 = st.columns([1, 1])
                with xc1:
                    preview_extract = st.button("👀 Preview rows", key="btn_extract_preview", type="secondary")
                with xc2:
                    export_extract  = st.button("💾 Export rows to CSV", key="btn_extract_export", type="primary")

                out_cols = extract_cols if extract_cols else columns

                if preview_extract or export_extract:
                    df_extracted = run_query(
                        folder_key,
                        out_cols,
                        filters={col_pick: pick_vals},
                        dual={},
                        max_rows=500 if preview_extract else None,
                    )

                    if df_extracted.empty:
                        st.warning("No rows found.")
                    elif preview_extract:
                        st.caption(f"Preview — first {len(df_extracted):,} rows")
                        st.dataframe(df_extracted, use_container_width=True, height=380)
                    else:
                        csv_bytes = df_extracted.to_csv(index=False).encode()
                        fname = f"rows_{col_pick}_{'_'.join(str(v)[:20] for v in pick_vals[:3])}.csv"
                        st.download_button(
                            label=f"📥 Download  ({len(df_extracted):,} rows)",
                            data=csv_bytes,
                            file_name=fname,
                            mime="text/csv",
                            key="dl_extract",
                        )
                        st.success(f"{len(df_extracted):,} rows ready to download.")


# ══════════════════════════════════════════════
# TAB 3 — Filter & Export
# ══════════════════════════════════════════════
with tab3:
    st.subheader("Filter & Export Data")
    st.caption("AND between columns, OR within a column's values.")

    sel_cols = st.multiselect(
        "Columns to include in output",
        options=columns,
        default=columns[:min(10, len(columns))],
        key="fe_cols",
    )

    st.markdown("---")

    filter_mode = st.radio(
        "Filter mode",
        ["Standard (AND per column)", "Dual filter (OR between two columns)"],
        horizontal=True,
        key="filter_mode",
    )

    filters: dict = {}
    dual:    dict = {}

    if filter_mode == "Standard (AND per column)":
        st.caption("Pick a column and one or more values. Rows must match ALL filters.")
        n_filters = st.number_input("Number of filters", 0, 6, 1, step=1, key="n_std")

        for i in range(int(n_filters)):
            fc1, fc2 = st.columns([2, 3])
            with fc1:
                fcol = st.selectbox(f"Filter {i+1} column", options=columns, key=f"std_col_{i}")
            with fc2:
                sug = (
                    st.session_state.last_unique_vals[:2000]
                    if st.session_state.last_unique_col == fcol else []
                )
                fvals = st.multiselect(
                    f"Filter {i+1} values (OR within)",
                    options=sug,
                    key=f"std_val_{i}",
                    placeholder="Select or type values ...",
                )
                if not sug:
                    st.caption("Tip: explore this column in Unique Values tab first to get suggestions here.")
            if fvals:
                filters[fcol] = fvals

    else:
        st.caption("Rows where **(Column A = value)  OR  (Column B = value)**.")

        da1, da2 = st.columns(2)
        with da1:
            st.markdown("**Column A**")
            dual_col_a  = st.selectbox("Column A", options=columns, key="dual_col_a")
            sug_a = st.session_state.last_unique_vals[:2000] if st.session_state.last_unique_col == dual_col_a else []
            dual_vals_a = st.multiselect("Values for A", options=sug_a, placeholder="Select or type ...", key="dual_val_a")
        with da2:
            st.markdown("**Column B**")
            dual_col_b  = st.selectbox("Column B", options=columns, key="dual_col_b")
            sug_b = st.session_state.last_unique_vals[:2000] if st.session_state.last_unique_col == dual_col_b else []
            dual_vals_b = st.multiselect("Values for B", options=sug_b, placeholder="Select or type ...", key="dual_val_b")

        dual = {"col_a": dual_col_a, "vals_a": dual_vals_a, "col_b": dual_col_b, "vals_b": dual_vals_b}

        st.markdown("**Optional extra AND filter**")
        extra_col = st.selectbox("Extra column (optional)", options=["(none)"] + columns, key="extra_col")
        if extra_col != "(none)":
            sug_e = st.session_state.last_unique_vals[:2000] if st.session_state.last_unique_col == extra_col else []
            extra_vals = st.multiselect("Extra values", options=sug_e, placeholder="Select or type ...", key="extra_vals")
            if extra_vals:
                filters[extra_col] = extra_vals

    st.markdown("---")

    n_preview = st.number_input(
        "Preview rows to show",
        min_value=10, max_value=10_000, value=200, step=10,
        key="n_preview",
    )

    col_a, col_b = st.columns([1, 1])
    with col_a:
        run_preview = st.button("👀 Preview", type="primary", key="btn_preview")
    with col_b:
        run_export  = st.button("💾 Export full CSV", key="btn_export")

    if run_preview:
        if not sel_cols:
            st.warning("Select at least one column.")
        else:
            df = run_query(folder_key, sel_cols, filters, dual, max_rows=int(n_preview))
            if df.empty:
                st.warning("No rows match your filters.")
            else:
                st.caption(f"Showing {len(df):,} rows")
                st.dataframe(df, use_container_width=True, height=480)

    if run_export:
        if not sel_cols:
            st.warning("Select at least one column.")
        else:
            df_all = run_query(folder_key, sel_cols, filters, dual, max_rows=None)
            if df_all.empty:
                st.warning("No rows match your filters.")
            else:
                csv_bytes = df_all.to_csv(index=False).encode()
                st.download_button(
                    label=f"📥 Download CSV  ({len(df_all):,} rows)",
                    data=csv_bytes,
                    file_name="export.csv",
                    mime="text/csv",
                    key="dl_export",
                )
                st.success(f"Ready — {len(df_all):,} rows matched.")


# ══════════════════════════════════════════════
# TAB 4 — Query String Analyzer  (NEW)
# ══════════════════════════════════════════════
with tab4:
    st.subheader("🔎 Query String Analyzer")
    st.caption(
        "Pick a column that contains URL query strings (e.g. `queryStr`, `qrystr`). "
        "The app parses every value, extracts `device_id` and `session_id`, "
        "and lets you explore their relationships."
    )

    # ── Step 1: column selection ──────────────────────────────
    st.markdown("#### Step 1 — Select query string column")
    qsa_col = st.selectbox(
        "Column containing query strings",
        options=columns,
        key="qsa_col_select",
        help="Choose the column whose values look like: session_id=xxx&device_id=yyy&...",
    )

    # Source options
    qsa_source = st.radio(
        "Data source",
        [
            "Use Unique Values already scanned (Tab 2)  — fast, uses aggregated counts",
            "Read raw parquet rows directly  — slower, full row-level data",
        ],
        key="qsa_source",
    )

    use_cached_uv = "Unique Values" in qsa_source

    if use_cached_uv:
        cached_udf = st.session_state.get(f"udf_{qsa_col}")
        if cached_udf is None:
            st.info(
                f"No unique values cached for `{qsa_col}` yet. "
                "Go to **Tab 2 → Unique Values**, select this column, and click 'Count unique values' first. "
                "Or switch to 'Read raw parquet rows' above."
            )
            st.stop()

    if st.button("🔍 Parse & Analyze", type="primary", key="qsa_parse_btn"):
        with st.spinner("Parsing query strings ..."):
            if use_cached_uv:
                raw_df = st.session_state[f"udf_{qsa_col}"].rename(columns={"value": "_raw_val"})
                records = []
                for _, row in raw_df.iterrows():
                    raw_val = str(row["_raw_val"])
                    cnt     = row.get("count", 1)
                    try:
                        parsed = dict(urllib.parse.parse_qsl(raw_val, keep_blank_values=True))
                    except Exception:
                        parsed = {}
                    parsed["_raw"]   = raw_val
                    parsed["_count"] = cnt
                    records.append(parsed)
                parsed_df = pd.DataFrame(records).fillna("")
            else:
                parsed_df = load_qrystr_from_parquet(folder_key, qsa_col)

        if parsed_df.empty:
            st.warning("No data found. Check column selection.")
        else:
            st.session_state.qsa_parsed_df = parsed_df
            st.session_state.qsa_column    = qsa_col
            # Detect available keys (exclude internal cols)
            meta_cols = {"_raw", "_count"}
            st.session_state.qsa_keys = [c for c in parsed_df.columns if c not in meta_cols]
            st.success(f"Parsed {len(parsed_df):,} unique values. Found keys: {', '.join(st.session_state.qsa_keys)}")

    # ── Results ──────────────────────────────────────────────
    parsed_df = st.session_state.get("qsa_parsed_df")
    qsa_keys  = st.session_state.get("qsa_keys", [])

    if parsed_df is not None and not parsed_df.empty:
        st.markdown("---")

        # ── Tabs within this tab ──────────────────────────────
        sub1, sub2, sub3, sub4 = st.tabs([
            "📊 Overview",
            "📱 Device ID Analysis",
            "🔗 Session ↔ Device Mapping",
            "🔍 Lookup by ID",
        ])

        # Helper: weighted unique count (respects _count)
        def weighted_nunique(df, col):
            if col not in df.columns:
                return 0
            sub = df[df[col].astype(str).str.strip() != ""]
            return sub[col].nunique()

        def weighted_total(df):
            return int(df["_count"].sum())

        # ── SUB-TAB 1: Overview ───────────────────────────────
        with sub1:
            st.markdown("#### Parsed Key Overview")
            st.caption("How many distinct non-empty values each key has across all records.")

            ov_rows = []
            for k in qsa_keys:
                non_empty = parsed_df[parsed_df[k].astype(str).str.strip() != ""]
                n_unique  = non_empty[k].nunique()
                n_filled  = int(non_empty["_count"].sum())
                n_total   = weighted_total(parsed_df)
                pct       = round(n_filled / n_total * 100, 1) if n_total else 0
                ov_rows.append({
                    "Key":          k,
                    "Unique values": n_unique,
                    "Filled rows":  n_filled,
                    "Fill %":       pct,
                })
            ov_df = pd.DataFrame(ov_rows).sort_values("Filled rows", ascending=False)
            st.dataframe(ov_df, use_container_width=True, hide_index=True)

            # Top values for any key
            st.markdown("---")
            st.markdown("#### Top values for a key")
            pick_key = st.selectbox("Select key", options=qsa_keys, key="ov_key_pick")
            if pick_key:
                top_df = (
                    parsed_df[parsed_df[pick_key].astype(str).str.strip() != ""]
                    .groupby(pick_key)["_count"]
                    .sum()
                    .reset_index()
                    .rename(columns={pick_key: "value", "_count": "count"})
                    .sort_values("count", ascending=False)
                    .head(50)
                )
                c1, c2 = st.columns([2, 1])
                with c1:
                    st.dataframe(top_df, use_container_width=True, height=380, hide_index=True)
                with c2:
                    st.bar_chart(top_df.head(10).set_index("value")["count"])

        # ── SUB-TAB 2: Device ID Analysis ─────────────────────
        with sub2:
            st.markdown("#### Device ID Analysis")

            if "device_id" not in parsed_df.columns:
                st.warning("No `device_id` key found in parsed query strings.")
            else:
                dev_df = parsed_df[parsed_df["device_id"].astype(str).str.strip() != ""].copy()
                dev_df["device_id"] = dev_df["device_id"].astype(str)

                n_unique_devices = dev_df["device_id"].nunique()
                n_rows_with_dev  = int(dev_df["_count"].sum())
                n_total          = weighted_total(parsed_df)

                d1, d2, d3 = st.columns(3)
                d1.metric("Unique device IDs",   f"{n_unique_devices:,}")
                d2.metric("Records with device", f"{n_rows_with_dev:,}")
                d3.metric("Coverage",            f"{n_rows_with_dev/n_total*100:.1f}%" if n_total else "—")

                st.markdown("---")

                # Sessions per device
                if "session_id" in dev_df.columns:
                    dev_df["session_id"] = dev_df["session_id"].astype(str)
                    dev_sess = (
                        dev_df[dev_df["session_id"].str.strip() != ""]
                        .groupby("device_id")
                        .agg(
                            session_count  = ("session_id", "nunique"),
                            total_events   = ("_count", "sum"),
                        )
                        .reset_index()
                        .sort_values("session_count", ascending=False)
                    )

                    # Add platform/device_name if available
                    for extra_key in ["platform", "device"]:
                        if extra_key in dev_df.columns:
                            top_val = (
                                dev_df.groupby("device_id")[extra_key]
                                .agg(lambda x: x.mode()[0] if len(x) > 0 else "")
                            )
                            dev_sess[extra_key] = dev_sess["device_id"].map(top_val)

                    st.markdown("#### Devices ranked by session count")
                    st.caption("Each device_id, how many unique sessions it has, and total event weight.")

                    # Distribution
                    sess_bins = pd.cut(
                        dev_sess["session_count"],
                        bins=[0, 1, 5, 10, 25, 50, 100, dev_sess["session_count"].max() + 1],
                        labels=["1", "2–5", "6–10", "11–25", "26–50", "51–100", "100+"],
                    )
                    dist_df = sess_bins.value_counts().sort_index().reset_index()
                    dist_df.columns = ["Sessions per device", "Device count"]

                    c_left, c_right = st.columns([1, 2])
                    with c_left:
                        st.markdown("**Session count distribution**")
                        st.dataframe(dist_df, use_container_width=True, hide_index=True)
                    with c_right:
                        st.bar_chart(dist_df.set_index("Sessions per device")["Device count"])

                    st.markdown("---")
                    top_n_devs = st.slider("Show top N devices by session count", 10, 500, 50, key="top_n_devs")
                    st.dataframe(
                        dev_sess.head(top_n_devs),
                        use_container_width=True,
                        height=420,
                        hide_index=True,
                    )

                    st.download_button(
                        "📥 Export device summary CSV",
                        data=dev_sess.to_csv(index=False).encode(),
                        file_name="device_summary.csv",
                        mime="text/csv",
                        key="dl_dev_summary",
                    )
                else:
                    st.info("No `session_id` key found. Showing device ID frequency only.")
                    dev_freq = (
                        dev_df.groupby("device_id")["_count"]
                        .sum()
                        .reset_index()
                        .rename(columns={"_count": "event_count"})
                        .sort_values("event_count", ascending=False)
                    )
                    st.dataframe(dev_freq, use_container_width=True, height=420, hide_index=True)

        # ── SUB-TAB 3: Session ↔ Device Mapping ───────────────
        with sub3:
            st.markdown("#### Session ↔ Device Relationship")

            if "session_id" not in parsed_df.columns or "device_id" not in parsed_df.columns:
                st.warning("Both `session_id` and `device_id` keys are required for this analysis.")
            else:
                # Filter to rows with both filled
                both_df = parsed_df[
                    (parsed_df["session_id"].astype(str).str.strip() != "") &
                    (parsed_df["device_id"].astype(str).str.strip()  != "")
                ].copy()
                both_df["session_id"] = both_df["session_id"].astype(str)
                both_df["device_id"]  = both_df["device_id"].astype(str)

                b1, b2, b3 = st.columns(3)
                b1.metric("Records with both IDs", f"{int(both_df['_count'].sum()):,}")
                b2.metric("Unique sessions",        f"{both_df['session_id'].nunique():,}")
                b3.metric("Unique devices",         f"{both_df['device_id'].nunique():,}")

                st.markdown("---")

                # Sessions linked to multiple devices
                sess_dev_cnt = both_df.groupby("session_id")["device_id"].nunique().reset_index()
                sess_dev_cnt.columns = ["session_id", "n_devices"]
                multi_dev_sess = sess_dev_cnt[sess_dev_cnt["n_devices"] > 1].sort_values("n_devices", ascending=False)

                st.markdown("#### ⚠️ Sessions linked to multiple device IDs")
                st.caption("A session appearing on more than one device could indicate account sharing or data anomalies.")
                if multi_dev_sess.empty:
                    st.success("✅ No sessions are linked to more than one device ID.")
                else:
                    st.warning(f"{len(multi_dev_sess):,} sessions appear on multiple devices.")
                    st.dataframe(multi_dev_sess, use_container_width=True, height=300, hide_index=True)
                    st.download_button(
                        "📥 Export multi-device sessions CSV",
                        data=multi_dev_sess.to_csv(index=False).encode(),
                        file_name="multi_device_sessions.csv",
                        mime="text/csv",
                        key="dl_multi_dev",
                    )

                st.markdown("---")

                # Devices linked to multiple sessions
                dev_sess_cnt = both_df.groupby("device_id")["session_id"].nunique().reset_index()
                dev_sess_cnt.columns = ["device_id", "n_sessions"]
                multi_sess_dev = dev_sess_cnt[dev_sess_cnt["n_sessions"] > 1].sort_values("n_sessions", ascending=False)

                st.markdown("#### 📱 Devices linked to multiple sessions")
                st.caption("Normal — a device typically has many sessions over time.")
                if multi_sess_dev.empty:
                    st.info("No devices with multiple sessions found.")
                else:
                    st.info(f"{len(multi_sess_dev):,} devices have more than one session.")
                    top_n = st.slider("Show top N", 10, 500, 50, key="top_n_multi_sess")
                    st.dataframe(multi_sess_dev.head(top_n), use_container_width=True, height=300, hide_index=True)
                    st.download_button(
                        "📥 Export multi-session devices CSV",
                        data=multi_sess_dev.to_csv(index=False).encode(),
                        file_name="multi_session_devices.csv",
                        mime="text/csv",
                        key="dl_multi_sess",
                    )

                st.markdown("---")

                # Full mapping table
                st.markdown("#### Full session → device mapping")
                mapping = (
                    both_df.groupby(["session_id", "device_id"])["_count"]
                    .sum()
                    .reset_index()
                    .rename(columns={"_count": "event_count"})
                    .sort_values("event_count", ascending=False)
                )
                st.caption(f"{len(mapping):,} unique (session, device) pairs")
                st.dataframe(mapping.head(500), use_container_width=True, height=380, hide_index=True)
                st.download_button(
                    "📥 Export full mapping CSV",
                    data=mapping.to_csv(index=False).encode(),
                    file_name="session_device_mapping.csv",
                    mime="text/csv",
                    key="dl_full_mapping",
                )

        # ── SUB-TAB 4: Lookup ─────────────────────────────────
        with sub4:
            st.markdown("#### 🔍 Lookup by ID")
            st.caption("Enter a device ID or session ID to see all associated records.")

            lookup_mode = st.radio(
                "Lookup by",
                ["device_id", "session_id"] + [k for k in qsa_keys if k not in ("device_id", "session_id")],
                horizontal=True,
                key="lookup_mode",
            )

            lookup_val = st.text_input(
                f"Enter {lookup_mode} value",
                placeholder=f"paste a {lookup_mode} here ...",
                key="lookup_val",
            )

            if lookup_val.strip() and lookup_mode in parsed_df.columns:
                results_lk = parsed_df[
                    parsed_df[lookup_mode].astype(str).str.contains(
                        re.escape(lookup_val.strip()), case=False, na=False
                    )
                ].copy()

                if results_lk.empty:
                    st.warning(f"No records found for `{lookup_mode}` = `{lookup_val}`")
                else:
                    st.success(f"Found {len(results_lk):,} matching record(s)")

                    # Key metrics for this ID
                    if "device_id" in results_lk.columns:
                        st.markdown(f"**Unique device IDs:** {results_lk['device_id'].nunique()}")
                    if "session_id" in results_lk.columns:
                        st.markdown(f"**Unique session IDs:** {results_lk['session_id'].nunique()}")

                    # Show all parsed fields
                    display_cols = [c for c in results_lk.columns if c != "_raw"]
                    st.dataframe(
                        results_lk[display_cols],
                        use_container_width=True,
                        height=400,
                        hide_index=True,
                    )

                    st.download_button(
                        f"📥 Export results for this {lookup_mode}",
                        data=results_lk[display_cols].to_csv(index=False).encode(),
                        file_name=f"lookup_{lookup_mode}_{lookup_val[:30]}.csv",
                        mime="text/csv",
                        key="dl_lookup",
                    )
            elif lookup_val.strip() and lookup_mode not in parsed_df.columns:
                st.warning(f"Key `{lookup_mode}` was not found in the parsed data.")