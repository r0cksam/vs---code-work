import streamlit as st
import duckdb
from pathlib import Path
import tempfile

st.set_page_config(layout="wide")
st.title("📊 Parquet Date-wise Explorer")


def duckdb_path(path_value):
    """Convert Windows/Linux path into safe DuckDB SQL string."""
    return "'" + str(path_value).replace("\\", "/").replace("'", "''") + "'"


# -----------------------------
# Root folder input
# -----------------------------
root_path = st.text_input(
    "Enter ROOT folder path",
    r"D:\Veto Logs Backup"
)

if not root_path:
    st.stop()

root = Path(root_path)

if not root.exists():
    st.error("Root path does not exist.")
    st.stop()

if not root.is_dir():
    st.error("Please enter a folder path, not a file path.")
    st.stop()

# -----------------------------
# Choose folder/file
# -----------------------------
st.subheader("📂 Select data source")

mode = st.radio(
    "Choose input type",
    [
        "Entire root folder",
        "Select one or more subfolders",
        "Select single parquet file"
    ]
)

parquet_source_sql = None

if mode == "Entire root folder":
    parquet_source_sql = duckdb_path(root / "**" / "*.parquet")

elif mode == "Select one or more subfolders":
    subfolders = sorted([f for f in root.iterdir() if f.is_dir()])

    if not subfolders:
        st.warning("No subfolders found inside root folder.")
        st.stop()

    selected_folders = st.multiselect(
        "Choose one or more subfolders",
        subfolders,
        format_func=lambda x: f"{x.name} ({len(list(x.glob('*.parquet')))} files)"
    )

    if not selected_folders:
        st.info("Select at least one folder.")
        st.stop()

    parquet_source_sql = "[" + ", ".join(
        duckdb_path(folder / "*.parquet")
        for folder in selected_folders
    ) + "]"

else:
    parquet_files = sorted(root.rglob("*.parquet"))

    if not parquet_files:
        st.warning("No parquet files found inside root folder.")
        st.stop()

    selected_file = st.selectbox(
        "Choose parquet file",
        parquet_files,
        format_func=lambda x: str(x.relative_to(root))
    )

    parquet_source_sql = duckdb_path(selected_file)

st.success(f"Using: {parquet_source_sql}")

# -----------------------------
# DuckDB
# -----------------------------
con = duckdb.connect()
con.execute("SET threads=12;")
con.execute("SET memory_limit='28GB';")
con.execute("SET preserve_insertion_order=false;")

# -----------------------------
# Timezone choice
# -----------------------------
timezone_mode = st.radio(
    "Treat date filter as",
    ["IST", "UTC"],
    horizontal=True
)

if timezone_mode == "IST":
    record_date_expr = """
        CAST(
            to_timestamp(TRY_CAST(reqTimeSec AS DOUBLE))
            AT TIME ZONE 'Asia/Kolkata'
            AS DATE
        )
    """
else:
    record_date_expr = """
        CAST(
            to_timestamp(TRY_CAST(reqTimeSec AS DOUBLE))
            AT TIME ZONE 'UTC'
            AS DATE
        )
    """

# -----------------------------
# Create optimized view
# -----------------------------
try:
    con.execute(f"""
        CREATE OR REPLACE VIEW data AS
        SELECT
            *,
            to_timestamp(TRY_CAST(reqTimeSec AS DOUBLE)) AS req_time_utc,
            to_timestamp(TRY_CAST(reqTimeSec AS DOUBLE)) AT TIME ZONE 'Asia/Kolkata' AS req_time_ist,
            {record_date_expr} AS record_date
        FROM read_parquet({parquet_source_sql})
        WHERE TRY_CAST(reqTimeSec AS DOUBLE) IS NOT NULL;
    """)
except Exception as e:
    st.error(f"Error loading parquet: {e}")
    con.close()
    st.stop()

st.success("✅ Data loaded")

# -----------------------------
# Dataset summary
# -----------------------------
st.subheader("📌 Dataset summary")

total_rows = con.execute("SELECT COUNT(*) FROM data").fetchone()[0]
column_df = con.execute("DESCRIBE data").fetchdf()
all_columns = column_df["column_name"].tolist()

col1, col2 = st.columns(2)
col1.metric("Valid rows", f"{total_rows:,}")
col2.metric("Columns", f"{len(all_columns):,}")

# -----------------------------
# Records per day
# -----------------------------
st.subheader("📊 Records per day")

daily_df = con.execute("""
    SELECT
        record_date,
        COUNT(*) AS records
    FROM data
    GROUP BY record_date
    ORDER BY record_date
""").fetchdf()

st.dataframe(daily_df, use_container_width=True)

if daily_df.empty:
    st.warning("No valid reqTimeSec values found.")
    con.close()
    st.stop()

min_date = daily_df["record_date"].min()
max_date = daily_df["record_date"].max()

# -----------------------------
# Date range preview
# -----------------------------
st.subheader("📅 Filter by date range")

date_range = st.date_input(
    "Select date range",
    [min_date, max_date],
    key="range_date"
)

if isinstance(date_range, (tuple, list)) and len(date_range) == 2:
    start_date, end_date = date_range

    filtered = con.execute(f"""
        SELECT *
        FROM data
        WHERE record_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
        LIMIT 100
    """).fetchdf()

    st.write(f"Showing first {len(filtered):,} rows")
    st.dataframe(filtered, use_container_width=True)

# -----------------------------
# Distinct records for selected date
# -----------------------------
st.subheader("🔍 Distinct records for selected date")

selected_date = st.date_input(
    "Pick a date",
    value=min_date,
    key="single_date"
)

distinct_mode = st.radio(
    "Distinct mode",
    ["Distinct selected columns", "Distinct full rows"]
)

display_limit = 100

if distinct_mode == "Distinct selected columns":
    selected_cols = st.multiselect(
        "Select columns for DISTINCT",
        all_columns,
        default=[c for c in ["record_date", "reqTimeSec"] if c in all_columns]
    )

    if not selected_cols:
        st.info("Select at least one column.")
        con.close()
        st.stop()

    cols_sql = ", ".join([f'"{c}"' for c in selected_cols])

    distinct_count_sql = f"""
        SELECT COUNT(*)
        FROM (
            SELECT DISTINCT {cols_sql}
            FROM data
            WHERE record_date = DATE '{selected_date}'
        )
    """

    preview_sql = f"""
        SELECT DISTINCT {cols_sql}
        FROM data
        WHERE record_date = DATE '{selected_date}'
        LIMIT {display_limit}
    """

    export_sql = f"""
        SELECT DISTINCT {cols_sql}
        FROM data
        WHERE record_date = DATE '{selected_date}'
    """

else:
    distinct_count_sql = f"""
        SELECT COUNT(*)
        FROM (
            SELECT DISTINCT *
            FROM data
            WHERE record_date = DATE '{selected_date}'
        )
    """

    preview_sql = f"""
        SELECT DISTINCT *
        FROM data
        WHERE record_date = DATE '{selected_date}'
        LIMIT {display_limit}
    """

    export_sql = f"""
        SELECT DISTINCT *
        FROM data
        WHERE record_date = DATE '{selected_date}'
    """

distinct_count = con.execute(distinct_count_sql).fetchone()[0]

st.metric("Total distinct records", f"{distinct_count:,}")

preview_df = con.execute(preview_sql).fetchdf()

st.write(f"Showing first {len(preview_df):,} records")
st.dataframe(preview_df, use_container_width=True)

# -----------------------------
# Export all distinct records
# -----------------------------
st.subheader("⬇️ Export")

if st.button("Prepare CSV export"):
    export_file = Path(tempfile.gettempdir()) / f"distinct_records_{selected_date}.csv"
    export_file_sql = export_file.as_posix().replace("'", "''")

    con.execute(f"""
        COPY (
            {export_sql}
        )
        TO '{export_file_sql}'
        (HEADER, DELIMITER ',');
    """)

    with open(export_file, "rb") as f:
        st.download_button(
            label="⬇️ Download all distinct records as CSV",
            data=f,
            file_name=f"distinct_records_{selected_date}.csv",
            mime="text/csv"
        )

# -----------------------------
# Custom SQL
# -----------------------------
st.subheader("🧪 Custom SQL")
st.caption("Use table name: data. The computed date column is record_date.")

sql = st.text_area(
    "SQL query",
    """
SELECT *
FROM data
LIMIT 100
""".strip()
)

if st.button("Run SQL"):
    try:
        custom_df = con.execute(sql).fetchdf()
        st.dataframe(custom_df, use_container_width=True)
    except Exception as e:
        st.error(f"SQL error: {e}")

con.close()