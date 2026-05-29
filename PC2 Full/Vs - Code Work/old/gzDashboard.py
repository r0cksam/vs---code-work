import streamlit as st
import pandas as pd
from pathlib import Path
import plotly.express as px

st.set_page_config(page_title="Akamai Dashboard", layout="wide", page_icon="📊")

st.title("📊 Akamai Batch Dashboard")
st.markdown(f"**Folder:** `D:\\VETO Logs\\parquet_output`  •  Memory-optimized version")

# ====================== CONFIG ======================
folder_path = st.text_input("Folder path", value=r"D:\VETO Logs\parquet_output")

# Memory-saving options
col_select_mode = st.radio("Column loading mode", 
                          ["Load All Columns (default)", "Select Specific Columns (recommended for large data)"], 
                          horizontal=True)

sample_frac = st.slider("Sample fraction (for quick testing)", 0.01, 1.0, 1.0, 0.01, 
                       help="Use < 1.0 if you run out of memory. 0.1 = 10% random sample")

# ====================== LOAD FILES (LIGHT) ======================
@st.cache_data
def get_parquet_files(folder):
    return list(Path(folder).rglob("*.parquet"))

if st.button("🔄 Scan & Load Data", type="primary"):
    files = get_parquet_files(folder_path)
    if not files:
        st.error("No .parquet files found!")
        st.stop()
    
    st.info(f"Found {len(files)} parquet files. Loading...")

    # Get all columns from first file for selection
    sample_df = pd.read_parquet(files[0], engine='pyarrow')
    all_cols = sorted(sample_df.columns.tolist())
    
    selected_cols = all_cols
    if col_select_mode.startswith("Select Specific"):
        selected_cols = st.multiselect("Choose columns to load (reduces memory a lot)", 
                                      options=all_cols, 
                                      default=[c for c in all_cols if any(k in c.lower() for k in ["date","time","ip","status","code","url","bytes","path","req","host"])])
    
    # Now load with selected columns + sampling
    dfs = []
    progress_bar = st.progress(0)
    for i, file in enumerate(files):
        try:
            df_temp = pd.read_parquet(
                file, 
                engine='pyarrow',
                columns=selected_cols if selected_cols else None
            )
            if sample_frac < 1.0:
                df_temp = df_temp.sample(frac=sample_frac, random_state=42)
            
            df_temp["source_file"] = file.name
            dfs.append(df_temp)
        except Exception as e:
            st.warning(f"Skipped {file.name}: {e}")
        
        progress_bar.progress((i+1)/len(files))
    
    if not dfs:
        st.error("Could not load any data.")
        st.stop()
    
    df = pd.concat(dfs, ignore_index=True)
    st.session_state.df = df
    st.success(f"✅ Loaded **{len(df):,} rows** × **{len(df.columns)} columns** from {len(files)} files")

if "df" not in st.session_state:
    st.info("👆 Click **Scan & Load Data** above")
    st.stop()

df = st.session_state.df.copy()

# ====================== FILTERS ======================
st.sidebar.header("🔎 Filters")

filter_cols = st.sidebar.multiselect(
    "Columns to filter on",
    options=sorted(df.columns),
    default=[c for c in df.columns if any(k in c.lower() for k in ["date","time","status","code","bytes","ip","url"])]
)

filtered_df = df.copy()

for col in filter_cols:
    st.sidebar.subheader(f"Filter: {col}")
    
    if pd.api.types.is_datetime64_any_dtype(df[col]) or any(x in col.lower() for x in ["date","time"]):
        try:
            dt = pd.to_datetime(filtered_df[col], errors='coerce')
            min_d, max_d = dt.min().date(), dt.max().date()
            d_range = st.sidebar.date_input(f"{col} range", (min_d, max_d))
            if len(d_range) == 2:
                filtered_df = filtered_df[(pd.to_datetime(filtered_df[col], errors='coerce').dt.date >= d_range[0]) &
                                          (pd.to_datetime(filtered_df[col], errors='coerce').dt.date <= d_range[1])]
        except:
            pass
    elif pd.api.types.is_numeric_dtype(df[col]):
        minv, maxv = float(df[col].min()), float(df[col].max())
        rng = st.sidebar.slider(f"{col}", minv, maxv, (minv, maxv))
        filtered_df = filtered_df[(filtered_df[col] >= rng[0]) & (filtered_df[col] <= rng[1])]
    elif df[col].nunique() <= 25:
        vals = sorted(df[col].dropna().astype(str).unique())
        sel = st.sidebar.multiselect(f"{col}", vals, default=vals)
        if sel:
            filtered_df = filtered_df[filtered_df[col].astype(str).isin(sel)]
    else:
        txt = st.sidebar.text_input(f"Search {col}", "")
        if txt:
            filtered_df = filtered_df[filtered_df[col].astype(str).str.contains(txt, case=False, na=False)]

if st.sidebar.button("Reset Filters"):
    st.rerun()

# ====================== DASHBOARD ======================
st.subheader(f"Filtered: **{len(filtered_df):,} rows**")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Rows", f"{len(filtered_df):,}")
c2.metric("Files", filtered_df["source_file"].nunique())

bytes_col = next((c for c in df.columns if any(k in c.lower() for k in ["bytes","byte","size","length"])), None)
if bytes_col:
    c3.metric("Total GB", f"{filtered_df[bytes_col].sum()/1e9:.2f}")

status_col = next((c for c in df.columns if any(k in c.lower() for k in ["status","code","response"])), None)
if status_col and len(filtered_df)>0:
    success = (filtered_df[status_col].astype(str).str.startswith(('2','3')).mean()*100)
    c4.metric("Success %", f"{success:.1f}")

tab1, tab2, tab3 = st.tabs(["Charts", "Data Table", "Download"])

with tab1:
    chart_type = st.selectbox("Chart", ["Bar (Top 20)", "Histogram", "Time Series (if date)", "Scatter"])
    x = st.selectbox("X column", filtered_df.columns)
    
    if chart_type == "Bar (Top 20)":
        fig = px.bar(filtered_df[x].value_counts().head(20))
    elif chart_type == "Histogram":
        fig = px.histogram(filtered_df, x=x)
    elif chart_type.startswith("Time Series") and ("date" in x.lower() or "time" in x.lower()):
        y_opts = [c for c in filtered_df.columns if pd.api.types.is_numeric_dtype(filtered_df[c])]
        y = st.selectbox("Y (numeric)", y_opts) if y_opts else None
        fig = px.line(filtered_df, x=x, y=y) if y else px.line(filtered_df, x=x)
    else:
        y_opts = [c for c in filtered_df.columns if pd.api.types.is_numeric_dtype(filtered_df[c])]
        y = st.selectbox("Y column", y_opts) if y_opts else None
        fig = px.scatter(filtered_df, x=x, y=y) if y else px.scatter(filtered_df, x=x)
    
    st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.dataframe(filtered_df, use_container_width=True, height=600)

with tab3:
    st.download_button("Download CSV", filtered_df.to_csv(index=False).encode(), "filtered_akamai.csv", "text/csv")
    st.download_button("Download Parquet", filtered_df.to_parquet(index=False), "filtered_akamai.parquet", "application/octet-stream")

st.caption("Memory-optimized Akamai Dashboard • Use column selection + sampling if still low on RAM")