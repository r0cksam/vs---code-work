from pdash.common import *
from pdash.core.data import *
from pdash.core.query_string import *
from pdash.analytics.behavior import *
from pdash.core.workflow import next_action
from pdash.core.disk_cache import clear_global_disk_cache, META_DB_PATH, GLOBAL_CACHE_DIR



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
for k, v in DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v


# ─────────────────────────────────────────────
# Folder scanner
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

with st.expander("⚙️ System status / next action", expanded=False):
    try:
        _idx = build_metadata_index(folder_key)
        st.write(f"**Metadata index:** {len(_idx.files):,} files, {_idx.total_rows:,} rows, {len(_idx.columns):,} columns")
        st.info(next_action(valid_folders, columns, st.session_state.get("fe_last_df") is not None, st.session_state.get("qsa_parsed_df") is not None))
        if st.checkbox("Show file inventory", value=False, key="show_file_inventory"):
            st.dataframe(_idx.as_files_df(), use_container_width=True, hide_index=True, height=260)
    except Exception as e:
        st.caption(f"System status unavailable: {e}")

with st.expander(f"📂 {n_fol} active folder(s)", expanded=False):
    for f in valid_folders:
        fpath = Path(f)
        cnt   = len(list(fpath.glob("*.parquet")))
        st.markdown(f"- **{fpath.name}** &nbsp; `{f}` &nbsp; ({cnt:,} files)")

st.markdown("---")

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📋 Columns",
    "🔍 Unique Values",
    "🎯 Filter & Export",
    "🔎 Query String Analyzer",
    "📺 User Behavior",
    "🌐 Global Behavior",
])


# ══════════════════════════════════════════════
# TAB 1 — Column browser
# ══════════════════════════════════════════════
with tab1:
    st.subheader("All Columns")
    st.caption("Union of all columns across selected folders. You can also profile how filled/empty each column is.")

    prof_exp = st.expander("📊 Column completeness profile", expanded=False)
    with prof_exp:
        st.caption(
            "Fast DuckDB-based profiling. Counts real NULL, blank values, `-`, and `^` as empty/null-like. "
            "Missing columns across files are treated as NULL via union_by_name."
        )

        profile_mode = st.radio(
            "Profile mode",
            ["Fast estimate (sample)", "Exact selected columns", "Exact all columns"],
            horizontal=True,
            key="col_profile_mode",
            help=(
                "Fast estimate samples rows. Exact selected columns scans only the columns you choose. "
                "Exact all columns is accurate but scans more data."
            ),
        )

        sample_rows = 100000
        if profile_mode == "Fast estimate (sample)":
            sample_rows = st.number_input(
                "Sample rows",
                min_value=1000,
                max_value=5_000_000,
                value=100_000,
                step=10_000,
                key="col_profile_sample_rows",
            )

        if profile_mode == "Exact selected columns":
            profile_cols = st.multiselect(
                "Columns to profile",
                options=columns,
                default=columns[:min(10, len(columns))],
                key="col_profile_cols",
            )
        else:
            profile_cols = columns

        pc1, pc2, pc3 = st.columns([1, 1, 1])
        with pc1:
            run_profile = st.button("Calculate filled / empty %", type="primary", key="btn_col_profile")
        with pc2:
            if st.session_state.get("col_profile_df") is not None:
                st.download_button(
                    "📥 Download column profile CSV",
                    data=st.session_state["col_profile_df"].to_csv(index=False).encode("utf-8"),
                    file_name="column_completeness_profile.csv",
                    mime="text/csv",
                    key="dl_col_profile",
                )
        with pc3:
            if st.button("🧹 Clear profile", key="btn_clear_col_profile"):
                st.session_state["col_profile_df"] = None
                st.session_state["col_profile_key"] = None
                st.rerun()

        if run_profile:
            if not profile_cols:
                st.warning("Select at least one column to profile.")
            else:
                try:
                    prof_df = column_completeness_profile(
                        folder_key,
                        list(profile_cols),
                        mode=profile_mode,
                        sample_rows=int(sample_rows),
                    )
                    st.session_state["col_profile_df"] = prof_df
                    st.session_state["col_profile_key"] = folder_key
                    st.success(f"Profile complete for {len(prof_df):,} column(s). Mode: {profile_mode}.")
                except Exception as e:
                    st.error(f"Column profile failed: {e}")

    search  = st.text_input("Search column name", placeholder="type to filter ...", key="col_search")
    cols_df = pd.DataFrame({
        "Column":    columns,
        "Data type": [col_types.get(c, "") for c in columns],
    })

    prof_df = st.session_state.get("col_profile_df")
    if prof_df is not None and not prof_df.empty and st.session_state.get("col_profile_key") == folder_key:
        cols_df = cols_df.merge(prof_df, on="Column", how="left")
        for c in ["Filled %", "Empty %"]:
            if c in cols_df.columns:
                cols_df[c] = cols_df[c].fillna(0).round(2)
        for c in ["Filled rows", "Empty/null-like rows", "Total rows checked"]:
            if c in cols_df.columns:
                cols_df[c] = cols_df[c].fillna(0).astype("int64")
    else:
        st.info("Optional: open **Column completeness profile** above and click **Calculate filled / empty %** to add filled/empty percentages to this table.")

    if search:
        cols_df = cols_df[cols_df["Column"].str.contains(search, case=False, na=False)]

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
                st.session_state["fe_last_df"] = df.copy()
                st.session_state["fe_last_source"] = "preview"

    if run_export:
        if not sel_cols:
            st.warning("Select at least one column.")
        else:
            df_all = run_query(folder_key, sel_cols, filters, dual, max_rows=None)
            if df_all.empty:
                st.warning("No rows match your filters.")
            else:
                if len(df_all) > EXPORT_MEMORY_SAFE_ROW_LIMIT:
                    st.warning(f"Large export: {len(df_all):,} rows. For very large extracts, prefer fewer columns/filters or implement streaming CSV output.")
                csv_bytes = df_all.to_csv(index=False).encode()
                st.download_button(
                    label=f"📥 Download CSV  ({len(df_all):,} rows)",
                    data=csv_bytes,
                    file_name="export.csv",
                    mime="text/csv",
                    key="dl_export",
                )
                st.success(f"Ready — {len(df_all):,} rows matched.")
                st.session_state["fe_last_df"] = df_all.copy()
                st.session_state["fe_last_source"] = "full export"


# ══════════════════════════════════════════════

    st.markdown("---")
    st.markdown("### 📊 Visualize Filtered Data")
    st.caption(
        "For Bar/Pie, choose **Full filtered dataset** to aggregate all matching parquet rows without loading them into RAM. "
        "Preview mode is still useful for quick histograms or time-series checks."
    )

    fe_plot_df = st.session_state.get("fe_last_df")
    fe_plot_source = st.session_state.get("fe_last_source", "")

    viz_scope = st.radio(
        "Chart data scope",
        ["Full filtered dataset (smart exact aggregation)", "Last preview/export result"],
        horizontal=True,
        key="fe_viz_scope",
    )

    if viz_scope.startswith("Full"):
        st.info("Full mode uses the filters currently selected above and scans parquet in batches. It is exact for Bar/Pie and does not need Preview first.")
        viz_columns = columns
    else:
        if fe_plot_df is None or fe_plot_df.empty:
            st.info("No preview/export data loaded yet. Click **Preview** above, or switch to **Full filtered dataset** mode.")
            viz_columns = []
        else:
            st.caption(f"Using last filtered result from **{fe_plot_source}**: {len(fe_plot_df):,} rows × {len(fe_plot_df.columns):,} columns")
            viz_columns = list(fe_plot_df.columns)

    if viz_columns:
        viz_c1, viz_c2, viz_c3, viz_c4 = st.columns([2, 1.2, 1, 1])
        with viz_c1:
            viz_col = st.selectbox("Column to visualize", options=viz_columns, key="fe_viz_col")
        with viz_c2:
            chart_type = st.selectbox(
                "Chart type",
                ["Bar graph", "Pie chart", "Auto", "Histogram", "Time series"],
                key="fe_chart_type",
                help="Full-dataset mode supports exact Bar/Pie. Histogram and Time series use last preview/export result.",
            )
        with viz_c3:
            top_n_viz = st.number_input("Top N", min_value=3, max_value=100, value=20, step=1, key="fe_viz_top_n")
        with viz_c4:
            include_other = st.checkbox("Group rest as Other", value=True, key="fe_viz_other")

        if viz_col:
            try:
                if viz_scope.startswith("Full"):
                    if chart_type not in ["Bar graph", "Pie chart", "Auto"]:
                        st.warning("Full filtered dataset mode currently supports exact Bar graph and Pie chart. Use Preview mode for Histogram or Time series.")
                    chosen_chart = "Bar graph" if chart_type == "Auto" else chart_type
                    if chosen_chart in ["Bar graph", "Pie chart"]:
                        run_full_chart = st.button("📊 Build chart from full filtered dataset", type="primary", key="fe_build_full_chart")
                        if run_full_chart:
                            vc = full_filtered_value_counts(
                                folder_key=folder_key,
                                target_col=viz_col,
                                filters=filters,
                                dual=dual,
                                top_n=int(top_n_viz),
                                include_other=bool(include_other),
                            )
                            st.session_state["fe_full_chart_df"] = vc
                            st.session_state["fe_full_chart_col"] = viz_col
                            st.session_state["fe_full_chart_type"] = chosen_chart
                        vc = st.session_state.get("fe_full_chart_df")
                        cached_col = st.session_state.get("fe_full_chart_col")
                        cached_type = st.session_state.get("fe_full_chart_type")
                        if vc is not None and not vc.empty and cached_col == viz_col and cached_type == chosen_chart:
                            if chosen_chart == "Pie chart":
                                if not PLOTLY_OK:
                                    st.warning("Plotly is required for pie charts. Run: pip install plotly")
                                    st.dataframe(vc, use_container_width=True, hide_index=True, height=260)
                                else:
                                    fig = px.pie(vc, names=viz_col, values="count", title=f"{viz_col} distribution — full filtered dataset")
                                    fig.update_traces(textposition="inside", textinfo="percent+label")
                                    fig.update_layout(height=520)
                                    st.plotly_chart(fig, use_container_width=True)
                            else:
                                if not PLOTLY_OK:
                                    st.bar_chart(vc.set_index(viz_col)["count"])
                                else:
                                    fig = px.bar(vc, x=viz_col, y="count", title=f"Top {int(top_n_viz)} values in {viz_col} — full filtered dataset")
                                    fig.update_layout(height=470, xaxis_tickangle=-35)
                                    st.plotly_chart(fig, use_container_width=True)
                            st.dataframe(vc, use_container_width=True, hide_index=True, height=280)
                            st.download_button(
                                "📥 Download full-dataset chart data CSV",
                                data=vc.to_csv(index=False).encode("utf-8"),
                                file_name=f"full_distribution_{viz_col}.csv",
                                mime="text/csv",
                                key="fe_dl_full_chart_data",
                            )
                        elif run_full_chart:
                            st.warning("No matching rows found for the selected filters/column.")

                else:
                    plot_df = fe_plot_df.copy()
                    max_plot_rows = 200_000
                    if len(plot_df) > max_plot_rows:
                        plot_df = plot_df.sample(max_plot_rows, random_state=42)
                        st.caption(f"Chart uses a random sample of {max_plot_rows:,} rows for responsiveness.")

                    s = plot_df[viz_col]
                    numeric_s = pd.to_numeric(s, errors="coerce")
                    is_numeric = numeric_s.notna().mean() > 0.75 if len(s) else False
                    chosen_chart = ("Histogram" if is_numeric else "Bar graph") if chart_type == "Auto" else chart_type

                    def _value_count_df(series: pd.Series, col_name: str, top_n: int, other: bool) -> pd.DataFrame:
                        full_vc = (
                            series.fillna("(null)")
                            .astype(str)
                            .replace("", "(blank)")
                            .value_counts()
                        )
                        vc = full_vc.head(int(top_n)).reset_index()
                        vc.columns = [col_name, "count"]
                        if other and len(full_vc) > int(top_n):
                            other_count = int(full_vc.iloc[int(top_n):].sum())
                            vc = pd.concat([vc, pd.DataFrame([{col_name: "Other", "count": other_count}])], ignore_index=True)
                        return vc

                    if chosen_chart == "Histogram":
                        tmp = pd.DataFrame({viz_col: numeric_s.dropna()})
                        if tmp.empty:
                            st.warning("No numeric values available for histogram. Try Bar graph or Pie chart for text columns.")
                        elif not PLOTLY_OK:
                            st.bar_chart(tmp[viz_col].value_counts().sort_index())
                        else:
                            fig = px.histogram(tmp, x=viz_col, nbins=50, title=f"Distribution of {viz_col}")
                            fig.update_layout(height=430)
                            st.plotly_chart(fig, use_container_width=True)

                    elif chosen_chart == "Time series":
                        time_candidates = [c for c in ["reqTimeSec", "event_time", "timestamp", "time"] if c in plot_df.columns]
                        time_col = st.selectbox("Time column", options=time_candidates + ([viz_col] if viz_col not in time_candidates else []), key="fe_time_col")
                        tnum = pd.to_numeric(plot_df[time_col], errors="coerce")
                        if tnum.notna().mean() > 0.75:
                            dt = pd.to_datetime(tnum, unit="s", errors="coerce")
                        else:
                            dt = pd.to_datetime(plot_df[time_col], errors="coerce")
                        ts_df = pd.DataFrame({"time": dt}).dropna()
                        if ts_df.empty:
                            st.warning("Could not parse a time column for time-series chart.")
                        else:
                            freq = st.selectbox("Bucket", ["1min", "5min", "15min", "1H", "1D"], index=2, key="fe_time_bucket")
                            ts_counts = ts_df.set_index("time").resample(freq).size().rename("records").reset_index()
                            if not PLOTLY_OK:
                                st.line_chart(ts_counts.set_index("time")["records"])
                            else:
                                fig = px.line(ts_counts, x="time", y="records", title=f"Records over time ({freq})")
                                fig.update_layout(height=430)
                                st.plotly_chart(fig, use_container_width=True)
                            st.download_button(
                                "📥 Download chart data CSV",
                                data=ts_counts.to_csv(index=False).encode("utf-8"),
                                file_name=f"time_distribution_{time_col}.csv",
                                mime="text/csv",
                                key="fe_dl_time_chart_data",
                            )

                    elif chosen_chart == "Pie chart":
                        vc = _value_count_df(s, viz_col, int(top_n_viz), bool(include_other))
                        if vc.empty:
                            st.warning("No values available to chart.")
                        elif not PLOTLY_OK:
                            st.warning("Plotly is required for pie charts. Run: pip install plotly")
                            st.dataframe(vc, use_container_width=True, hide_index=True, height=260)
                        else:
                            fig = px.pie(vc, names=viz_col, values="count", title=f"{viz_col} distribution")
                            fig.update_traces(textposition="inside", textinfo="percent+label")
                            fig.update_layout(height=520)
                            st.plotly_chart(fig, use_container_width=True)
                            st.dataframe(vc, use_container_width=True, hide_index=True, height=260)
                            st.download_button(
                                "📥 Download chart data CSV",
                                data=vc.to_csv(index=False).encode("utf-8"),
                                file_name=f"pie_distribution_{viz_col}.csv",
                                mime="text/csv",
                                key="fe_dl_pie_chart_data",
                            )

                    else:
                        vc = _value_count_df(s, viz_col, int(top_n_viz), bool(include_other))
                        if vc.empty:
                            st.warning("No values available to chart.")
                        elif not PLOTLY_OK:
                            st.bar_chart(vc.set_index(viz_col)["count"])
                        else:
                            fig = px.bar(vc, x=viz_col, y="count", title=f"Top {int(top_n_viz)} values in {viz_col}")
                            fig.update_layout(height=470, xaxis_tickangle=-35)
                            st.plotly_chart(fig, use_container_width=True)
                        if not vc.empty:
                            st.dataframe(vc, use_container_width=True, hide_index=True, height=260)
                            st.download_button(
                                "📥 Download chart data CSV",
                                data=vc.to_csv(index=False).encode("utf-8"),
                                file_name=f"bar_distribution_{viz_col}.csv",
                                mime="text/csv",
                                key="fe_dl_bar_chart_data",
                            )
            except Exception as e:
                st.error(f"Could not build chart: {e}")
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
        _qbar = st.progress(0, text="Starting parse ...")
        _qph  = st.empty()
        _qbar.progress(10, text="📋 Reading cached values ...")
        if use_cached_uv:
            raw_df = st.session_state[f"udf_{qsa_col}"].rename(columns={"value": "_raw_val"})
            records = []
            total_qsa = len(raw_df)
            for qi, (_, row) in enumerate(raw_df.iterrows()):
                pct = 10 + int(qi / max(total_qsa, 1) * 70)
                if qi % 500 == 0:
                    _qbar.progress(pct, text=f"📋 Parsing query strings ... {qi:,} / {total_qsa:,}")
                raw_val = str(row["_raw_val"])
                cnt     = row.get("count", 1)
                try:
                    parsed = dict(urllib.parse.parse_qsl(raw_val, keep_blank_values=True))
                except Exception:
                    parsed = {}
                parsed["_raw"]   = raw_val
                parsed["_count"] = cnt
                records.append(parsed)
            _qbar.progress(85, text="🔧 Building DataFrame ...")
            parsed_df = pd.DataFrame(records).fillna("")
        else:
            _qbar.progress(20, text="📂 Reading raw parquet rows ...")
            parsed_df = load_qrystr_from_parquet(folder_key, qsa_col)

        _qbar.progress(90, text="🔍 Identifying keys ...")
        if parsed_df.empty:
            _qbar.empty()
            _qph.empty()
            st.warning("No data found. Check column selection.")
        else:
            meta_cols = {"_raw", "_count"}
            st.session_state.qsa_parsed_df = parsed_df
            st.session_state.qsa_column    = qsa_col
            st.session_state.qsa_keys = [c for c in parsed_df.columns if c not in meta_cols]
            _qbar.progress(100, text="✅ Parse complete!")
            time.sleep(0.3)
            _qbar.empty()
            _qph.empty()
            st.success(f"Parsed {len(parsed_df):,} unique values. Found keys: {', '.join(st.session_state.qsa_keys)}")

    # ── Results ──────────────────────────────────────────────
    parsed_df = st.session_state.get("qsa_parsed_df")
    qsa_keys  = st.session_state.get("qsa_keys", [])

    if parsed_df is not None and not parsed_df.empty:
        st.markdown("---")

        # ── Pure channel list from parsed query strings ──────────────────────────────
        if "channel" in parsed_df.columns or "channel_name" in parsed_df.columns:
            with st.expander("✅ Pure Channel List (cleaned from queryStr)", expanded=False):
                ch_src = parsed_df.copy()
                if "channel" not in ch_src.columns:
                    ch_src["channel"] = ""
                if "channel_name" not in ch_src.columns:
                    ch_src["channel_name"] = ""
                ch_src["raw_channel_qsa"] = ch_src["channel"].astype(str).where(
                    ch_src["channel"].astype(str).str.strip() != "",
                    ch_src["channel_name"].astype(str)
                )
                ch_src = ch_src[ch_src["raw_channel_qsa"].astype(str).str.strip() != ""].copy()
                if not ch_src.empty:
                    if "platform" not in ch_src.columns:
                        ch_src["platform"] = ""
                    if "device" not in ch_src.columns:
                        ch_src["device"] = ""
                    ch_src["pure_channel"] = ch_src.apply(
                        lambda r: normalize_channel_name_smart(r.get("raw_channel_qsa", ""), r.get("platform", ""), r.get("device", "")),
                        axis=1,
                    )
                    ch_master = (
                        ch_src.groupby(["pure_channel", "raw_channel_qsa", "platform", "device"], dropna=False)["_count"]
                        .sum()
                        .reset_index()
                        .rename(columns={"raw_channel_qsa": "raw_channel", "device": "device_name", "_count": "records"})
                        .sort_values("records", ascending=False)
                    )
                    ch_pure = (
                        ch_master.groupby("pure_channel", dropna=False)["records"]
                        .sum()
                        .reset_index()
                        .sort_values("records", ascending=False)
                    )
                    st.caption("Use this list for pure business channels. The normal Unique Values tab shows raw log values, so it will still show FireTV/AndroidTV suffixes.")
                    cpa, cpb = st.columns(2)
                    with cpa:
                        st.markdown("**Pure channels**")
                        st.dataframe(ch_pure, use_container_width=True, hide_index=True, height=300)
                        st.download_button(
                            "📥 Download pure channel list",
                            data=ch_pure.to_csv(index=False).encode("utf-8"),
                            file_name="pure_channel_list.csv",
                            mime="text/csv",
                            key="qsa_dl_pure_channels",
                        )
                    with cpb:
                        st.markdown("**Raw → clean mapping**")
                        st.dataframe(ch_master, use_container_width=True, hide_index=True, height=300)
                        st.download_button(
                            "📥 Download raw-to-clean mapping",
                            data=ch_master.to_csv(index=False).encode("utf-8"),
                            file_name="channel_raw_to_clean_mapping.csv",
                            mime="text/csv",
                            key="qsa_dl_channel_mapping",
                        )

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
                        f"📥 Export parsed results for this {lookup_mode}",
                        data=results_lk[display_cols].to_csv(index=False).encode(),
                        file_name=f"lookup_{lookup_mode}_{lookup_val[:30]}.csv",
                        mime="text/csv",
                        key="dl_lookup",
                    )

                    # ── Fetch full parquet rows ──────────────────────
                    st.markdown("---")
                    st.markdown("#### 📂 Fetch full parquet rows for this lookup")
                    st.caption(
                        "This goes back to the original parquet files and returns **every column** "
                        f"for rows where `{qsa_col}` contains your lookup value — not just the parsed fields."
                    )

                    # Column selector for full row fetch
                    full_row_cols = st.multiselect(
                        "Columns to include  (leave empty = all columns)",
                        options=columns,
                        default=[],
                        key="full_row_cols",
                        help="Leave empty to get every column from the parquet file.",
                    )
                    out_full_cols = full_row_cols if full_row_cols else columns

                    # The filter: match the qrystr column value containing the lookup value
                    # We use the _raw query strings from the lookup results as the exact filter values
                    raw_values_to_match = results_lk["_raw"].astype(str).tolist()

                    fr1, fr2 = st.columns(2)
                    with fr1:
                        btn_preview_full = st.button(
                            "👀 Preview full rows (first 500)",
                            key="btn_preview_full_rows",
                            type="secondary",
                        )
                    with fr2:
                        btn_export_full = st.button(
                            "💾 Export all full rows to CSV",
                            key="btn_export_full_rows",
                            type="primary",
                        )

                    if btn_preview_full or btn_export_full:
                        max_r = 500 if btn_preview_full else None
                        df_full = run_query(
                            folder_key,
                            sel_cols=out_full_cols,
                            filters={qsa_col: raw_values_to_match},
                            dual={},
                            max_rows=max_r,
                            progress_label="Fetching full rows from parquet",
                        )

                        if df_full.empty:
                            st.warning("No matching rows found in parquet files.")
                        elif btn_preview_full:
                            st.caption(f"Preview — {len(df_full):,} rows × {len(df_full.columns)} columns")
                            st.dataframe(df_full, use_container_width=True, height=450, hide_index=True)
                        else:
                            csv_full = df_full.to_csv(index=False).encode()
                            fname_full = f"full_rows_{lookup_mode}_{lookup_val[:30]}.csv"
                            st.download_button(
                                label=f"📥 Download full rows CSV  ({len(df_full):,} rows × {len(df_full.columns)} cols)",
                                data=csv_full,
                                file_name=fname_full,
                                mime="text/csv",
                                key="dl_full_rows",
                            )
                            st.success(f"{len(df_full):,} full rows ready to download.")

            elif lookup_val.strip() and lookup_mode not in parsed_df.columns:
                st.warning(f"Key `{lookup_mode}` was not found in the parsed data.")


# ══════════════════════════════════════════════
# TAB 5 — User Behavior Dashboard
# ══════════════════════════════════════════════
with tab5:
    st.subheader("📺 User Behavior Dashboard")

    if not DUCKDB_OK:
        st.error("DuckDB not installed. Run: `pip install duckdb`")
        st.stop()
    if not PLOTLY_OK:
        st.error("Plotly not installed. Run: `pip install plotly`")
        st.stop()

    # ── Column mapping UI ─────────────────────────────────────
    with st.expander("⚙️ Column Mapping  (expand to configure)", expanded=False):
        st.caption(
            "Map the roles the dashboard needs to your actual parquet column names. "
            "Leave blank to skip optional columns."
        )
        col_map = st.session_state.ub_col_map
        none_opt = ["(not available)"] + columns

        def col_picker(role, label, required=True):
            opts   = columns if required else none_opt
            stored = col_map.get(role, role)
            try:
                idx = opts.index(stored)
            except ValueError:
                idx = 0
            chosen = st.selectbox(label, opts, index=idx, key=f"ub_cm_{role}")
            col_map[role] = chosen if chosen != "(not available)" else ""
            return col_map[role]

        r1, r2, r3 = st.columns(3)
        with r1:
            col_picker("queryStr",   "🔑 Query string column *",    required=True)
            col_picker("reqTimeSec", "🕐 Timestamp column (epoch) *", required=True)
            col_picker("reqPath",    "🛣️  Request path column *",    required=True)
        with r2:
            col_picker("UA",         "🖥️  User-Agent column",        required=False)
            col_picker("cliIP",      "🌐 Client IP column",          required=False)
            col_picker("asn",        "📡 ASN column",                required=False)
        with r3:
            col_picker("statusCode",      "✅ Status code column",     required=False)
            col_picker("transferTimeMSec","⚡ Transfer time (ms)",      required=False)
            col_picker("downloadTime",    "⬇️  Download time column",  required=False)

        st.session_state.ub_col_map = col_map
        if st.button("💾 Save column mapping", key="ub_save_map"):
            st.success("Column mapping saved.")

    st.markdown("---")

    # Validate required columns are mapped
    cm = st.session_state.ub_col_map
    qs_col  = cm.get("queryStr", "")
    ts_col  = cm.get("reqTimeSec", "")
    path_col= cm.get("reqPath", "")

    if not qs_col or not ts_col or not path_col:
        st.warning("Please configure at least the Query string, Timestamp, and Request path columns above.")
        st.stop()

    # Build parquet glob list for DuckDB
    pq_files = collect_files(valid_folders)
    if not pq_files:
        st.warning("No parquet files found in selected folders.")
        st.stop()

    pq_glob = [str(f) for f in pq_files]

    # ── Session gap setting ───────────────────────────────────
    session_gap = st.number_input(
        "Session gap (minutes) — gap larger than this starts a new session",
        min_value=5, max_value=180, value=20, step=5,
        key="ub_session_gap",
    )

    # ── Device ID selection ─────────────────────────────────────
    st.markdown("#### Step 1 — Select a device")

    qsa_df = st.session_state.get("qsa_parsed_df")
    qsa_device_ids = []
    if qsa_df is not None and not qsa_df.empty and "device_id" in qsa_df.columns:
        qsa_device_ids = sorted(
            qsa_df["device_id"]
            .dropna()
            .astype(str)
            .loc[lambda s: s.str.strip() != ""]
            .unique()
            .tolist()
        )

    if qsa_device_ids:
        st.session_state.ub_device_ids = qsa_device_ids

    ub_scan_col1, ub_scan_col2 = st.columns([3, 1])
    with ub_scan_col1:
        if st.session_state.ub_device_ids:
            source_label = "from QSA cache" if qsa_device_ids else "from parquet scan"
            st.caption(f"{len(st.session_state.ub_device_ids):,} device IDs ready ({source_label})")
    with ub_scan_col2:
        scan_devices_btn = st.button("🔄 Refresh from parquet", key="ub_scan_btn")

    if scan_devices_btn:
        _bar  = st.progress(0, text="Preparing scan ...")
        _ph   = st.empty()
        _stages = [
            (5,  "Opening parquet files"),
            (20, "Scanning queryStr column"),
            (50, "Extracting device_id values"),
            (75, "Deduplicating results"),
            (90, "Sorting device list"),
        ]
        ids = ub_staged_progress(_bar, _ph, _stages,
                                  lambda: ub_get_device_ids(pq_glob, qs_col))
        st.session_state.ub_device_ids = ids or []
        if not ids:
            st.warning("No device_id values found. Check your queryStr column mapping.")

    device_ids_list = st.session_state.ub_device_ids

    manual_device = st.text_input(
        "Or type a device_id manually",
        placeholder="paste device_id here ...",
        key="ub_manual_device",
    )

    if manual_device.strip():
        selected_device = manual_device.strip()
    elif device_ids_list:
        selected_device = st.selectbox("Select device_id", device_ids_list, key="ub_device_select")
    else:
        st.info("Parse query strings first in the Query String Analyzer, or refresh from parquet, or type a device_id manually.")
        st.stop()

    # ── Step 1B — Resolve available date range without loading all device rows ─────
    # Speed fix: earlier versions loaded the selected device for all dates first.
    # That is slow for heavy devices. Now we only fetch min/max dates, then load
    # the selected date range when the dashboard actually refreshes.
    if st.session_state.get("ub_loaded_device") != selected_device:
        min_d, max_d = ub_get_device_date_range(pq_glob, selected_device, cm)
        if min_d is None or max_d is None:
            st.warning("Could not find a date range for this device. Try another device_id or check column mapping.")
            st.stop()
        st.session_state["ub_loaded_device"] = selected_device
        st.session_state["ub_active_device"] = selected_device
        st.session_state["ub_device_min_date"] = min_d
        st.session_state["ub_device_max_date"] = max_d
        st.session_state["ub_tmp_df"] = None
        st.session_state["ub_sessionized_df"] = None
        st.session_state["ub_sessionized_key"] = None
        st.session_state["ub_window_df"] = None
        st.session_state["ub_sess_df"] = pd.DataFrame()
        st.session_state["ub_date_range"] = (min_d, max_d)
        st.session_state["ub_start_time"] = dtime(0, 0)
        st.session_state["ub_end_time"] = dtime(23, 59)
        st.session_state["ub_min_req"] = 1

    # ── Step 2 — Pick date range before loading rows ─────────────────────────────
    st.markdown("#### Step 2 — Pick date range")
    min_allowed_date = st.session_state.get("ub_device_min_date")
    max_allowed_date = st.session_state.get("ub_device_max_date")

    if min_allowed_date is None or max_allowed_date is None:
        st.warning("No date range available for this device.")
        st.stop()

    date_range = st.date_input(
        "Date range",
        min_value=min_allowed_date,
        max_value=max_allowed_date,
        key="ub_date_range",
    )
    start_date, end_date = (date_range if isinstance(date_range, tuple) and len(date_range)==2
                            else (min_allowed_date, max_allowed_date))

    tc1, tc2, tc3 = st.columns(3)
    with tc1:
        start_time = st.time_input("Start time", key="ub_start_time")
    with tc2:
        end_time   = st.time_input("End time", key="ub_end_time")
    with tc3:
        min_req_content = st.number_input(
            "Min requests per content", min_value=1, max_value=1000, step=1,
            key="ub_min_req",
        )

    st.caption("Changing device/date/time refreshes the dashboard automatically. Rows are loaded only for the selected date range.")

    # ── Step 3 — Auto-run dashboard ──────────────────────────
    current_params = (
        selected_device,
        str(start_date),
        str(end_date),
        str(start_time),
        str(end_time),
        int(session_gap),
        int(min_req_content),
    )

    if st.session_state.get("ub_run_params") != current_params or st.session_state.get("ub_window_df") is None:
        _rbar = st.progress(0, text="Refreshing dashboard ...")
        _rph  = st.empty()

        # Load only the selected date range for this device.
        load_key = (selected_device, str(start_date), str(end_date))
        tmp_df = st.session_state.get("ub_tmp_df")
        if st.session_state.get("ub_loaded_range_key") != load_key or tmp_df is None or tmp_df.empty:
            _rbar.progress(10, text="📂 Loading selected device/date range ...")
            _rph.caption("⏳ Filtering parquet by device_id and selected date range only")
            _raw = ub_load_device(pq_glob, selected_device, str(start_date), str(end_date), cm)
            tmp_df = ub_enrich(_raw)
            if tmp_df.empty:
                st.session_state["ub_window_df"] = pd.DataFrame()
                st.session_state["ub_sess_df"] = pd.DataFrame()
                st.session_state["ub_run_params"] = current_params
                _rbar.empty(); _rph.empty()
                st.warning("No data found for this device in the selected date range.")
                st.stop()
            st.session_state["ub_tmp_df"] = tmp_df
            st.session_state["ub_loaded_range_key"] = load_key
            st.session_state["ub_sessionized_df"] = None
            st.session_state["ub_sessionized_key"] = None
        else:
            _rbar.progress(10, text="⚡ Reusing loaded device/date data ...")

        sessionized_key = (selected_device, str(start_date), str(end_date), int(session_gap))
        sessionized_df = st.session_state.get("ub_sessionized_df")
        if st.session_state.get("ub_sessionized_key") != sessionized_key or sessionized_df is None:
            _rbar.progress(45, text=f"🔗 Building sessions for {len(tmp_df):,} rows ...")
            _rph.caption("⏳ Applying session logic and watch-time estimation to the selected range")
            sessionized_df = ub_build_sessions(tmp_df.copy(), gap_minutes=int(session_gap))
            sessionized_df = ub_estimate_watch_minutes(sessionized_df, cap_seconds=WATCH_GAP_CAP_SECONDS)
            st.session_state["ub_sessionized_df"] = sessionized_df
            st.session_state["ub_sessionized_key"] = sessionized_key
        else:
            _rbar.progress(45, text="⚡ Reusing cached sessionized range data ...")

        _rbar.progress(80, text="⏱️ Applying time window ...")
        window_df = sessionized_df[
            (sessionized_df["time_only"] >= start_time) &
            (sessionized_df["time_only"] <= end_time)
        ].copy()

        if window_df.empty:
            st.session_state["ub_window_df"] = pd.DataFrame()
            st.session_state["ub_sess_df"] = pd.DataFrame()
            st.session_state["ub_run_params"] = current_params
            _rbar.empty(); _rph.empty()
            st.warning("No activity in selected time window.")
            st.stop()

        st.session_state["ub_window_df"] = window_df
        st.session_state["ub_sess_df"] = ub_session_summary(window_df)
        st.session_state["ub_run_params"] = current_params

        _rbar.progress(100, text="✅ Dashboard refreshed")
        time.sleep(0.2)
        _rbar.empty()
        _rph.empty()

    window_df = st.session_state.get("ub_window_df")
    sess_df   = st.session_state.get("ub_sess_df", pd.DataFrame())

    if window_df is None or window_df.empty:
        st.stop()

    st.markdown("---")

    # ── KPIs ─────────────────────────────────────────────────
    first_seen     = window_df["event_time"].min()
    last_seen      = window_df["event_time"].max()
    n_sessions     = window_df["session_key"].nunique()
    est_watch_hrs  = round(window_df["watch_min_est"].sum() / 60, 2)
    top_content    = window_df["content_label"].mode().iloc[0] if not window_df["content_label"].mode().empty else "—"
    top_channel    = window_df["channel_name"].mode().iloc[0]  if not window_df["channel_name"].mode().empty  else "—"
    top_platform   = window_df["platform"].mode().iloc[0]       if not window_df["platform"].mode().empty      else "—"
    top_dev_type   = window_df["device_type"].mode().iloc[0]    if not window_df["device_type"].mode().empty   else "—"

    k1,k2,k3,k4,k5,k6 = st.columns(6)
    k1.metric("Rows",             f"{len(window_df):,}")
    k2.metric("Sessions",         f"{n_sessions:,}")
    k3.metric("Est. Watch Hrs",   str(est_watch_hrs))
    k4.metric("Top Channel",      str(top_channel)[:20])
    k5.metric("Top Content",      str(top_content)[:20])
    k6.metric("Device Type",      str(top_dev_type))

    with st.expander("📋 Window summary", expanded=True):
        # ── Geo/ISP lookup on unique IPs ──────────────────────
        # Speed/stability fix: do NOT call external geo service automatically.
        # It is slow, rate-limited, and can hang on large IP lists.
        _geo_data    = {}
        _top_ip      = ""
        _geo_summary = {}
        do_geo_lookup = False
        if "cliIP" in window_df.columns:
            _all_ips = window_df["cliIP"].dropna().astype(str).tolist()
            if _all_ips:
                _top_ip = window_df["cliIP"].mode().iloc[0] if not window_df["cliIP"].mode().empty else ""
                _unique_ips = window_df["cliIP"].dropna().unique().tolist()
                st.caption(f"Geo lookup is optional. {len(_unique_ips):,} unique IP(s) found in this window.")
                do_geo_lookup = st.button("🌍 Lookup Geo/ISP for this window", key="ub_geo_lookup_btn")
                if do_geo_lookup:
                    with st.spinner(f"🌍 Looking up geo info for {len(_unique_ips):,} unique IP(s) ..."):
                        _geo_data = ub_lookup_geo(_unique_ips)
                    if _top_ip in _geo_data:
                        _geo_summary = ub_format_geo_summary(_geo_data[_top_ip])

        # ── Layout: 3 columns ─────────────────────────────────
        ws1, ws2, ws3 = st.columns(3)

        with ws1:
            st.markdown("**🖥️ Device**")
            st.write(f"**device_id:** `{selected_device}`")
            if "device_name_qs" in window_df.columns and not window_df["device_name_qs"].dropna().empty:
                dname = window_df["device_name_qs"].mode().iloc[0]
                if dname:
                    st.write(f"**Device name:** {dname}")
            st.write(f"**Device type:** {top_dev_type}")
            st.write(f"**Platform:** {top_platform}")
            st.markdown("**📅 Time window**")
            st.write(f"**From:** {start_date} {start_time}")
            st.write(f"**To:**   {end_date} {end_time}")
            st.write(f"**First event:** {first_seen}")
            st.write(f"**Last event:**  {last_seen}")

        with ws2:
            st.markdown("**🌍 Location (most common IP)**")
            if _geo_summary:
                loc = _geo_summary.get("location", "")
                if loc and loc != "Unknown":
                    st.write(f"**Location:** {loc}")
                if _geo_summary.get("country"):
                    st.write(f"**Country:** {_geo_summary['country']}")
                if _geo_summary.get("region"):
                    st.write(f"**State / Region:** {_geo_summary['region']}")
                if _geo_summary.get("city"):
                    st.write(f"**City:** {_geo_summary['city']}")
                if _geo_summary.get("timezone"):
                    st.write(f"**Timezone:** {_geo_summary['timezone']}")
                if _top_ip:
                    st.write(f"**IP (most common):** `{_top_ip}`")
            elif _top_ip:
                st.write(f"**IP (most common):** `{_top_ip}`")
                st.caption("_Geo lookup unavailable (offline or rate limited)_")
            else:
                st.caption("_No IP data available_")

            # All unique IPs seen
            if "cliIP" in window_df.columns:
                _n_ips = window_df["cliIP"].dropna().nunique()
                if _n_ips > 1:
                    st.caption(f"{_n_ips} unique IPs seen in this window")
                    with st.expander("Show all IPs"):
                        _ip_counts = (window_df["cliIP"].value_counts(dropna=True)
                                      .reset_index().rename(columns={"cliIP": "IP", "count": "requests"}))
                        # Add geo info per IP
                        _ip_counts["location"] = _ip_counts["IP"].map(
                            lambda ip: ub_format_geo_summary(_geo_data.get(str(ip), {})).get("location", "")
                        )
                        st.dataframe(_ip_counts, hide_index=True, use_container_width=True)

        with ws3:
            st.markdown("**📡 Network / ISP**")
            if _geo_summary:
                if _geo_summary.get("isp"):
                    st.write(f"**ISP:** {_geo_summary['isp']}")
                if _geo_summary.get("asn_name"):
                    st.write(f"**ASN:** {_geo_summary['asn_name']}")
            elif "asn" in window_df.columns and not window_df["asn"].dropna().empty:
                _top_asn = window_df["asn"].mode().iloc[0]
                st.write(f"**ASN (raw):** {_top_asn}")
                st.caption("_ISP name unavailable — geo lookup offline_")

            # ASN breakdown if multiple
            if "asn" in window_df.columns:
                _n_asns = window_df["asn"].dropna().nunique()
                if _n_asns > 1:
                    st.caption(f"{_n_asns} unique ASNs seen")
                    with st.expander("Show all ASNs"):
                        _asn_df = (window_df["asn"].value_counts(dropna=True)
                                   .reset_index().rename(columns={"asn": "ASN", "count": "requests"}))
                        # Add ISP name from geo data where possible
                        _ip_to_asn = {}
                        for _, _row in window_df[["cliIP","asn"]].dropna().iterrows():
                            _ip_to_asn[str(_row["asn"])] = _geo_data.get(str(_row["cliIP"]), {}).get("as", "")
                        _asn_df["ISP / Org"] = _asn_df["ASN"].astype(str).map(
                            lambda a: _ip_to_asn.get(str(a), "")
                        )
                        st.dataframe(_asn_df, hide_index=True, use_container_width=True)

            st.markdown("**📊 Activity**")
            st.write(f"**Total requests:** {len(window_df):,}")
            st.write(f"**Sessions:** {n_sessions:,}")
            st.write(f"**Est. watch hours:** {est_watch_hrs}")
            st.write(f"**Top channel:** {top_channel}")

    st.markdown("---")

    # ── Section 1: Watching timeline ─────────────────────────
    st.subheader("1) Watching history over time")
    hover_cols = [c for c in ["reqPath","content_title","quality","session_key","asn","platform"] if c in window_df.columns]
    fig1 = px.scatter(
        window_df, x="event_time", y="channel_name", color="content_label",
        hover_data=hover_cols, title="Watching history over time",
    )
    fig1.update_traces(marker=dict(opacity=0.85, size=9))
    fig1.update_layout(height=520)
    st.plotly_chart(fig1, use_container_width=True)

    # ── Section 1B: Date vs Time of day ──────────────────────
    st.subheader("1B) Date vs Time of Day (behavior pattern)")
    fig1b = px.scatter(
        window_df, x="watch_date", y="watch_time_min", color="channel_name",
        hover_data=[c for c in ["event_time","content_label","quality","session_key","asn","platform"] if c in window_df.columns],
        title="User behavior by date and time of day",
    )
    fig1b.update_traces(marker=dict(size=8, opacity=0.8))
    tick_vals = list(range(0, 1441, 60))
    tick_text = [f"{h:02d}:00" for h in range(25)]
    fig1b.update_yaxes(tickvals=tick_vals, ticktext=tick_text, range=[0, 1440])
    fig1b.update_layout(height=550, xaxis_title="Date", yaxis_title="Time of Day")
    st.plotly_chart(fig1b, use_container_width=True)

    # ── Section 2: Minute-by-minute ──────────────────────────
    st.subheader("2) Minute-by-minute activity")
    min_act = (
        window_df.set_index("event_time")
        .resample("1min").size()
        .rename("requests").reset_index()
    )
    fig2 = px.line(min_act, x="event_time", y="requests", title="Activity intensity")
    fig2.update_layout(height=350)
    st.plotly_chart(fig2, use_container_width=True)

    # ── Section 3: Content summary ───────────────────────────
    st.subheader("3) Content watched in this time range")
    content_window = (
        window_df.groupby(["content_label","channel_name"])
        .agg(
            requests      = ("reqPath",      "size"),
            est_watch_min = ("watch_min_est","sum"),
            first_seen    = ("event_time",   "min"),
            last_seen     = ("event_time",   "max"),
            sessions      = ("session_key",  "nunique"),
        )
        .reset_index()
        .sort_values(["est_watch_min","requests"], ascending=False)
    )
    content_window = content_window[content_window["requests"] >= min_req_content]
    st.dataframe(content_window, use_container_width=True, height=300, hide_index=True)

    fig3 = px.bar(
        content_window.head(15), x="est_watch_min", y="content_label",
        color="channel_name", orientation="h",
        title="Top content by estimated watch minutes",
    )
    fig3.update_layout(height=500, yaxis={"categoryorder":"total ascending"})
    st.plotly_chart(fig3, use_container_width=True)

    # ── Section 4: Paths / endpoints ─────────────────────────
    st.subheader("4) Request paths / endpoints")
    paths_window = (
        window_df.groupby("reqPath")
        .agg(
            requests      = ("reqPath",      "size"),
            first_seen    = ("event_time",   "min"),
            last_seen     = ("event_time",   "max"),
            est_watch_min = ("watch_min_est","sum"),
        )
        .reset_index()
        .sort_values(["requests","est_watch_min"], ascending=False)
        .head(100)
    )
    st.dataframe(paths_window, use_container_width=True, height=300, hide_index=True)

    # ── Section 5: Session drill-down ────────────────────────
    st.subheader("5) Session drill-down")
    session_opts = sorted(window_df["session_key"].unique().tolist())
    sel_session  = st.selectbox("Select session", session_opts, key="ub_sel_session")
    sess_win_df  = window_df[window_df["session_key"] == sel_session].copy()

    ss1,ss2,ss3,ss4 = st.columns(4)
    ss1.metric("Rows",          f"{len(sess_win_df):,}")
    dur = round((sess_win_df["event_time"].max() - sess_win_df["event_time"].min()).total_seconds() / 60, 2) if len(sess_win_df) > 1 else 0
    ss2.metric("Duration (min)", dur)
    ss3.metric("Unique Content", f"{sess_win_df['content_label'].nunique():,}")
    ss4.metric("Est. Watch Min", round(sess_win_df["watch_min_est"].sum(), 2))

    s_cols = [c for c in ["event_time","channel_name","content_label","reqPath","quality","platform","asn","statusCode","watch_min_est","session_key"] if c in sess_win_df.columns]
    st.dataframe(sess_win_df[s_cols].sort_values("event_time"), use_container_width=True, height=320, hide_index=True)

    # ── Section 6: Content switching ─────────────────────────
    st.subheader("6) Content switching moments")
    sw = window_df.sort_values("event_time").copy()
    sw["prev_content"] = sw["content_label"].shift(1)
    switches = sw[sw["content_label"] != sw["prev_content"]][
        [c for c in ["event_time","prev_content","content_label","channel_name","session_key"] if c in sw.columns]
    ].copy()
    st.dataframe(switches, use_container_width=True, height=250, hide_index=True)

    # ── Section 7: Watch starts ───────────────────────────────
    st.subheader("7) Likely watch starts (first event per session)")
    watch_starts = (
        window_df.sort_values("event_time")
        .groupby("session_key").first().reset_index()
        [[c for c in ["session_key","event_time","content_label","channel_name","platform"] if c in window_df.columns]]
    )
    st.dataframe(watch_starts, use_container_width=True, height=250, hide_index=True)

    # ── Section 8: Network / ASN ─────────────────────────────
    if "asn" in window_df.columns and not window_df["asn"].dropna().empty:
        st.subheader("8) Network usage (ASN)")
        asn_df = window_df["asn"].value_counts(dropna=False).reset_index()
        asn_df.columns = ["asn","requests"]
        c_asn1, c_asn2 = st.columns([2, 1])
        with c_asn1:
            st.dataframe(asn_df, use_container_width=True, hide_index=True)
        with c_asn2:
            st.bar_chart(asn_df.head(10).set_index("asn")["requests"])

    # ── Section 9: Raw events ─────────────────────────────────
    st.subheader("9) Raw events in selected window")
    raw_cols = [c for c in ["event_time","session_key","channel_name","content_label","content_title",
                             "platform","device_type","reqPath","quality","asn","cliIP",
                             "statusCode","transferTimeMSec","downloadTime","queryStr"] if c in window_df.columns]
    st.dataframe(window_df[raw_cols].sort_values("event_time"), use_container_width=True, height=420, hide_index=True)

    # ── Downloads ─────────────────────────────────────────────
    st.markdown("---")
    date_label = f"{start_date}_to_{end_date}"
    dl1, dl2, dl3 = st.columns(3)
    with dl1:
        st.download_button(
            "📥 Raw events CSV",
            data=window_df.to_csv(index=False).encode("utf-8"),
            file_name=f"behavior_{selected_device}_{date_label}.csv",
            mime="text/csv", key="ub_dl_raw",
        )
    with dl2:
        if not sess_df.empty:
            st.download_button(
                "📥 Session summary CSV",
                data=sess_df.to_csv(index=False).encode("utf-8"),
                file_name=f"sessions_{selected_device}_{date_label}.csv",
                mime="text/csv", key="ub_dl_sess",
            )
    with dl3:
        st.download_button(
            "📥 Content summary CSV",
            data=content_window.to_csv(index=False).encode("utf-8"),
            file_name=f"content_{selected_device}_{date_label}.csv",
            mime="text/csv", key="ub_dl_content",
        )

    # Speed fix: build the PDF only when the user asks for it.
    if st.button("📄 Generate User Behavior Report PDF", key="ub_generate_pdf"):
        with st.spinner("Building PDF report ..."):
            pdf_bytes = ub_build_pdf_report(
                window_df=window_df,
                sess_df=sess_df,
                content_window=content_window,
                selected_device=selected_device,
                start_date=start_date,
                end_date=end_date,
                top_channel=top_channel,
                top_content=top_content,
                est_watch_hrs=est_watch_hrs,
                n_sessions=n_sessions,
            )
        st.download_button(
            "📥 Download User Behavior Report (PDF)",
            data=pdf_bytes,
            file_name=f"user_behavior_report_{selected_device}_{date_label}.pdf",
            mime="application/pdf",
            key="ub_dl_pdf",
        )

    with st.expander("ℹ️ How to read this dashboard"):
        st.markdown("""
- **device_id** is extracted from the query string column via regex.
- **Sessions** are detected by time gaps, channel changes, IP/ASN shifts and platform changes.
- **Estimated watch minutes** = time gap between consecutive events, capped to avoid inflation.
- **Content switching** shows every moment the user changed what they were watching.
- **Watch starts** = first event in each session — likely when the user pressed play.
- This is behavioral estimation from CDN/access logs, not player telemetry.
        """)

# ══════════════════════════════════════════════
# TAB 6 — Global Behavior Dashboard
# ══════════════════════════════════════════════
with tab6:
    st.subheader("🌐 Global Behavior Dashboard")
    st.caption(
        "Preloads Content + Channel analytics once for the selected record date range. "
        "After preload, switching Focus / Top N / section does not rerun heavy queries."
    )

    if not DUCKDB_OK:
        st.error("DuckDB not installed. Run: `pip install duckdb`")
        st.stop()
    if not PLOTLY_OK:
        st.error("Plotly not installed. Run: `pip install plotly`")
        st.stop()

    gb_cm = st.session_state.ub_col_map
    gb_qs = gb_cm.get("queryStr", "")
    gb_ts = gb_cm.get("reqTimeSec", "")
    gb_path = gb_cm.get("reqPath", "")
    if not gb_qs or not gb_ts or not gb_path:
        st.warning("Please configure Query string, Timestamp, and Request path in the User Behavior column mapping first.")
        st.stop()

    gb_files = collect_files(valid_folders)
    if not gb_files:
        st.warning("No parquet files found in selected folders.")
        st.stop()
    gb_glob = [str(f) for f in gb_files]
    gb_files_key = tuple(map(str, gb_glob))

    min_record_date, max_record_date = gb_get_available_date_range(gb_glob, gb_cm)
    if min_record_date is None or max_record_date is None:
        st.warning("No behavior records with valid timestamp/session_id were found in the selected folders.")
        st.stop()

    # Initialize full available record range once per selected folder set.
    gb_range_key = (gb_files_key, str(min_record_date), str(max_record_date), gb_qs, gb_ts, gb_path)
    if st.session_state.get("gb_available_range_key") != gb_range_key:
        st.session_state["gb_available_range_key"] = gb_range_key
        st.session_state["gb_date_range"] = (min_record_date, max_record_date)
        # Force one fresh preload when selected folders/record range changes.
        st.session_state.pop("gb_preload_bundle_key", None)
        st.session_state.pop("gb_preload_bundle", None)

    st.markdown("#### Loaded record range")
    r1, r2, r3, r4 = st.columns([1, 1, 1, 1])
    with r1:
        st.metric("First record date", str(min_record_date))
    with r2:
        st.metric("Last record date", str(max_record_date))
    with r3:
        gb_daypart_mode = st.selectbox("Day-part mode", ["Default", "TV Style"], index=1, key="gb_daypart_mode")
    with r4:
        force_reload = st.button("🔄 Reload Global Cache", key="gb_force_reload")

    gb_dates = st.date_input(
        "Date range used for Global preload",
        min_value=min_record_date,
        max_value=max_record_date,
        key="gb_date_range",
        help="Default is the full first-to-last date range available in the records. Changing this range reloads the cached global bundle once.",
    )
    gb_start, gb_end = gb_dates if isinstance(gb_dates, tuple) and len(gb_dates) == 2 else (min_record_date, max_record_date)
    gb_start_s, gb_end_s = str(gb_start), str(gb_end)

    preload_top_n = 50
    bundle_key = (gb_files_key, gb_start_s, gb_end_s, gb_daypart_mode, preload_top_n, tuple(sorted(gb_cm.items())))

    if force_reload:
        st.session_state.pop("gb_preload_bundle_key", None)
        st.session_state.pop("gb_preload_bundle", None)
        gb_preload_global_bundle.clear()
        try:
            deleted_cache_files = clear_global_disk_cache()
            st.toast(f"Cleared {deleted_cache_files} persisted Global cache file(s).")
        except Exception as e:
            st.warning(f"Could not clear disk cache: {e}")

    if st.session_state.get("gb_preload_bundle_key") != bundle_key or st.session_state.get("gb_preload_bundle") is None:
        with st.spinner("Preloading Global Behavior once for this date range ..."):
            bundle = gb_preload_global_bundle(
                gb_glob,
                gb_cm,
                gb_start_s,
                gb_end_s,
                daypart_mode=gb_daypart_mode,
                preload_top_n=preload_top_n,
            )
        st.session_state["gb_preload_bundle_key"] = bundle_key
        st.session_state["gb_preload_bundle"] = bundle
    else:
        bundle = st.session_state["gb_preload_bundle"]
        st.success("⚡ Using cached Global Behavior bundle. Option clicks below will not rerun heavy parquet queries.")

    if bundle.get("_disk_cache_hit"):
        st.info(f"💾 Loaded Global Behavior from persistent disk cache: `{bundle.get('_disk_cache_path', '')}`")
    else:
        st.caption(f"Persistent cache folder: `{GLOBAL_CACHE_DIR}` | metadata DB: `{META_DB_PATH}`")

    cov = bundle.get("coverage", {})
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Rows in range", f"{int(cov.get('total_rows', 0)):,}")
    c2.metric("Behavior rows", f"{int(cov.get('behavior_rows', 0)):,}")
    c3.metric("Coverage", f"{float(cov.get('coverage_pct', 0)):.2f}%")
    c4.metric("Sessions", f"{int(cov.get('sessions', 0)):,}")
    c5.metric("Devices", f"{int(cov.get('devices', 0)):,}")

    if bundle.get("errors"):
        with st.expander("⚠️ Preload warnings", expanded=False):
            for err in bundle["errors"]:
                st.warning(err)

    if int(cov.get("behavior_rows", 0)) == 0:
        st.warning("No behavior rows found in this date range. Try widening the range.")
        st.stop()

    st.markdown("---")

    # Pure Channel Master is now loaded automatically in the bundle.
    with st.expander("✅ Pure Channel Master (auto-loaded)", expanded=False):
        ch_master_df = bundle.get("channel_master", pd.DataFrame())
        if ch_master_df is None or ch_master_df.empty:
            st.info("No channel metadata found for this range.")
        else:
            pure_summary = (
                ch_master_df.groupby("pure_channel", dropna=False)
                .agg(requests=("requests", "sum"), sessions=("sessions", "sum"), devices=("devices", "sum"), raw_variants=("raw_channel", "nunique"))
                .reset_index()
                .sort_values(["requests", "sessions"], ascending=False)
            )
            pc1, pc2 = st.columns([1, 1])
            with pc1:
                st.markdown("**Pure channel summary**")
                st.dataframe(pure_summary, use_container_width=True, hide_index=True, height=340)
                st.download_button(
                    "📥 Download Pure Channel Summary CSV",
                    data=pure_summary.to_csv(index=False).encode("utf-8"),
                    file_name=f"pure_channel_summary_{gb_start_s}_to_{gb_end_s}.csv",
                    mime="text/csv",
                    key="gb_dl_pure_channel_summary",
                )
            with pc2:
                st.markdown("**Raw → clean audit mapping**")
                st.dataframe(ch_master_df, use_container_width=True, hide_index=True, height=340)
                st.download_button(
                    "📥 Download Raw-to-Pure Channel Mapping CSV",
                    data=ch_master_df.to_csv(index=False).encode("utf-8"),
                    file_name=f"channel_raw_to_clean_{gb_start_s}_to_{gb_end_s}.csv",
                    mime="text/csv",
                    key="gb_dl_raw_pure_channel_mapping",
                )

    st.markdown("---")
    st.markdown("### Global analytics")
    st.caption("Everything below reads from the preloaded cache. Changing these controls only changes display.")

    g1, g2, g3 = st.columns([1, 1, 2])
    with g1:
        gb_entity = st.radio("Focus", ["Content", "Channel"], horizontal=True, key="gb_entity")
    with g2:
        gb_top_n = st.selectbox("Top N", [5, 10, 15, 20, 30, 50], index=1, key="gb_top_n")
    with g3:
        gb_section = st.radio(
            "Section",
            ["🌅 Day-part", "📌 Stickiness", "⏱️ Retention Proxy", "🔁 Switching"],
            horizontal=True,
            key="gb_section_picker",
        )

    entity_key = "content" if gb_entity == "Content" else "channel"
    entity_payload = bundle.get(entity_key, {})

    def _limit_daypart(df: pd.DataFrame, top_n: int) -> pd.DataFrame:
        if df is None or df.empty or "rn" not in df.columns:
            return pd.DataFrame() if df is None else df
        return df[df["rn"] <= int(top_n)].copy()

    def _limit_rows(df: pd.DataFrame, top_n: int) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        return df.head(int(top_n)).copy()

    if gb_section == "🌅 Day-part":
        day_df = _limit_daypart(entity_payload.get("daypart", pd.DataFrame()), int(gb_top_n))
        if day_df.empty:
            st.info("No day-part data found.")
        else:
            st.dataframe(day_df, use_container_width=True, hide_index=True, height=340)
            fig_dp = px.bar(day_df, x="day_part", y="sessions", color="entity_name", title=f"Top {gb_top_n} {gb_entity.lower()} by day-part")
            fig_dp.update_layout(height=480)
            st.plotly_chart(fig_dp, use_container_width=True)
            st.download_button("📥 Download day-part CSV", data=day_df.to_csv(index=False).encode("utf-8"), file_name=f"global_daypart_{gb_start_s}_to_{gb_end_s}.csv", mime="text/csv", key="gb_dl_daypart")

    elif gb_section == "📌 Stickiness":
        sticky_df = _limit_rows(entity_payload.get("stickiness", pd.DataFrame()), int(gb_top_n))
        if sticky_df.empty:
            st.info("No stickiness data found.")
        else:
            st.dataframe(sticky_df, use_container_width=True, hide_index=True, height=360)
            fig_st = px.scatter(sticky_df, x="requests", y="avg_watch_per_request", size="sessions", color="label", hover_name="entity_name", title=f"{gb_entity} stickiness")
            fig_st.update_layout(height=520)
            st.plotly_chart(fig_st, use_container_width=True)
            st.download_button("📥 Download stickiness CSV", data=sticky_df.to_csv(index=False).encode("utf-8"), file_name=f"global_stickiness_{gb_start_s}_to_{gb_end_s}.csv", mime="text/csv", key="gb_dl_stickiness")

    elif gb_section == "⏱️ Retention Proxy":
        ret_df = entity_payload.get("retention", pd.DataFrame())
        if ret_df is None or ret_df.empty:
            st.info("No retention proxy data found.")
        else:
            # Keep only Top N entities by total sessions from the preloaded table.
            top_entities = (
                ret_df.groupby("entity_name")["sessions"].sum().sort_values(ascending=False).head(int(gb_top_n)).index.tolist()
            )
            ret_df = ret_df[ret_df["entity_name"].isin(top_entities)].copy()
            st.dataframe(ret_df, use_container_width=True, hide_index=True, height=320)
            fig_rt = px.bar(ret_df, x="entity_name", y="sessions", color="watch_bucket", title=f"{gb_entity} retention buckets")
            fig_rt.update_layout(height=520, xaxis_title=gb_entity)
            st.plotly_chart(fig_rt, use_container_width=True)
            st.download_button("📥 Download retention CSV", data=ret_df.to_csv(index=False).encode("utf-8"), file_name=f"global_retention_{gb_start_s}_to_{gb_end_s}.csv", mime="text/csv", key="gb_dl_retention")

    elif gb_section == "🔁 Switching":
        if gb_entity != "Channel":
            st.info("Switching is channel-based. Switch Focus to **Channel** to view channel transitions.")
        switch_payload = bundle.get("channel", {}).get("switching", (pd.DataFrame(), pd.DataFrame(), pd.DataFrame()))
        trans_df, rate_df, sess_bucket_df = switch_payload
        trans_df = _limit_rows(trans_df, int(gb_top_n))
        rate_df = _limit_rows(rate_df, int(gb_top_n))
        s1, s2 = st.columns([2, 1])
        with s1:
            st.markdown("**Top channel transitions**")
            st.dataframe(trans_df, use_container_width=True, hide_index=True, height=300)
            st.download_button("📥 Download transitions CSV", data=trans_df.to_csv(index=False).encode("utf-8"), file_name=f"global_switch_transitions_{gb_start_s}_to_{gb_end_s}.csv", mime="text/csv", key="gb_dl_transitions")
        with s2:
            st.markdown("**Switches per session**")
            st.dataframe(sess_bucket_df, use_container_width=True, hide_index=True)
        if not rate_df.empty:
            fig_sw = px.bar(rate_df, x="switch_away_pct", y="channel_name", orientation="h", title="Channels with highest switch-away rate")
            fig_sw.update_layout(height=500, yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig_sw, use_container_width=True)
            st.download_button("📥 Download switch-away CSV", data=rate_df.to_csv(index=False).encode("utf-8"), file_name=f"global_switch_away_{gb_start_s}_to_{gb_end_s}.csv", mime="text/csv", key="gb_dl_switch_rate")
