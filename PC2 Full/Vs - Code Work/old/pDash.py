"""
parquet_explorer.py  -  Visual Parquet Explorer (multi-folder + smart browser)
===============================================================================
Run:  streamlit run parquet_explorer.py

pip install streamlit pandas pyarrow
"""

import json
import time
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
    "selected_folders": [],   # list of str — folders chosen for analysis
    "last_unique_col":  None,
    "last_unique_vals": [],
    "browser_root":     "",   # root path being browsed
    "scan_results":     [],   # list of dicts from scan
}
for k, v in DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v


# ─────────────────────────────────────────────
# Folder scanner
# ─────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def scan_root(root: str) -> list:
    """
    Walk root up to 4 levels deep using os.scandir (fast).
    Only checks filenames — never opens any file.
    Returns list of dicts for every folder that contains .parquet files.
    """
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


@st.cache_data(show_spinner="Scanning unique values ...", max_entries=32)
def unique_values(folder_key: str, column: str) -> pd.DataFrame:
    folders = folder_key.split("|")
    files   = collect_files(folders)
    counter = {}
    for f in files:
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
    if not counter:
        return pd.DataFrame(columns=["value", "count", "% of rows"])
    df    = pd.DataFrame(list(counter.items()), columns=["value", "count"])
    df    = df.sort_values("count", ascending=False).reset_index(drop=True)
    total = df["count"].sum()
    df["% of rows"] = (df["count"] / total * 100).round(2)
    return df


def apply_all_filters(tbl, filters: dict, dual: dict):
    mask = None

    # dual OR
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

    # standard AND
    std = build_mask(tbl, filters)
    if std is not None:
        mask = std if mask is None else pc.and_(mask, std)

    if mask is not None:
        tbl = tbl.filter(mask)
    return tbl


def run_query(folder_key: str, sel_cols: list, filters: dict, dual: dict, max_rows=None) -> pd.DataFrame:
    folders   = folder_key.split("|")
    files     = collect_files(folders)
    frames    = []
    collected = 0
    for f in files:
        if max_rows is not None and collected >= max_rows:
            break
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
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ─────────────────────────────────────────────
# SIDEBAR  —  Smart Folder Browser
# ─────────────────────────────────────────────

with st.sidebar:
    st.title("🗂️ Parquet Explorer")
    st.markdown("---")

    # ── Mode toggle ──────────────────────────
    mode = st.radio(
        "Folder selection mode",
        ["🔍 Browse from root", "✏️ Type paths manually"],
        horizontal=False,
        key="folder_mode",
    )

    st.markdown("---")

    # ════════════════════════════════
    # MODE A — Smart browser
    # ════════════════════════════════
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

            # Select All / Clear buttons
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

            # One button per found folder
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

    # ════════════════════════════════
    # MODE B — Manual paths
    # ════════════════════════════════
    else:
        st.markdown("**Type folder paths manually**")
        st.caption("One path per box. All must contain .parquet files.")

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

        # validate and push to selected_folders
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
    st.caption("Tabs:\n1. Columns — see all column names\n2. Unique Values — explore per column\n3. Filter & Export — query and download")


# ─────────────────────────────────────────────
# Resolve final valid folders
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

tab1, tab2, tab3 = st.tabs(["📋 Columns", "🔍 Unique Values", "🎯 Filter & Export"])


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
        with st.spinner("Scanning all files ..."):
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
        st.caption("Rows where **(Column A = value)  OR  (Column B = value)**. Useful when same value lives in two columns.")

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
            with st.spinner("Loading ..."):
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
            with st.spinner("Scanning all files ... this may take a while."):
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