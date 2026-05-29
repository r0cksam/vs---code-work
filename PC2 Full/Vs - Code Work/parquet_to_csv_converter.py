import os
from pathlib import Path

import pandas as pd
import streamlit as st

try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None


st.set_page_config(page_title="Parquet to CSV Converter", page_icon="📄", layout="wide")

st.title("📄 Parquet to CSV Converter")
st.caption("Convert parquet files to CSV with a loading/progress bar.")


def scan_parquet_files(folder: str, recursive: bool = True):
    root = Path(folder).expanduser()
    if not root.exists() or not root.is_dir():
        return []
    if recursive:
        return sorted(root.rglob("*.parquet"))
    return sorted(root.glob("*.parquet"))


def safe_csv_name(parquet_path: Path) -> str:
    return parquet_path.with_suffix(".csv").name


def convert_single_parquet_to_csv(parquet_file: Path, output_csv: Path, batch_size: int = 100_000):
    """Convert one parquet file to CSV in batches.

    Uses pyarrow row groups where possible so large files do not load fully into memory.
    """
    if pq is None:
        raise RuntimeError("pyarrow is not installed. Run: pip install pyarrow")

    pf = pq.ParquetFile(parquet_file)
    total_row_groups = pf.num_row_groups

    output_csv.parent.mkdir(parents=True, exist_ok=True)

    first_write = True
    rows_written = 0

    progress = st.progress(0, text=f"Starting {parquet_file.name}")
    status = st.empty()

    for rg_idx in range(total_row_groups):
        table = pf.read_row_group(rg_idx)
        df = table.to_pandas()

        df.to_csv(
            output_csv,
            mode="w" if first_write else "a",
            index=False,
            header=first_write,
            encoding="utf-8",
        )

        first_write = False
        rows_written += len(df)

        pct = int((rg_idx + 1) / max(total_row_groups, 1) * 100)
        progress.progress(
            pct,
            text=f"{parquet_file.name}: row group {rg_idx + 1}/{total_row_groups}",
        )
        status.info(f"Rows written: {rows_written:,}")

    progress.progress(100, text=f"Done: {parquet_file.name}")
    status.success(f"Saved: {output_csv} | Rows: {rows_written:,}")
    return rows_written


def convert_folder_to_csv(files, output_folder: Path):
    output_folder.mkdir(parents=True, exist_ok=True)

    overall = st.progress(0, text="Starting folder conversion")
    log_box = st.empty()

    results = []
    total_files = len(files)

    for i, parquet_file in enumerate(files, start=1):
        out_csv = output_folder / safe_csv_name(parquet_file)

        st.markdown(f"### Converting {i}/{total_files}: `{parquet_file.name}`")
        try:
            rows = convert_single_parquet_to_csv(parquet_file, out_csv)
            results.append({
                "file": str(parquet_file),
                "csv": str(out_csv),
                "rows": rows,
                "status": "OK",
            })
        except Exception as exc:
            results.append({
                "file": str(parquet_file),
                "csv": str(out_csv),
                "rows": 0,
                "status": f"ERROR: {exc}",
            })
            st.error(f"Failed: {parquet_file.name} — {exc}")

        overall.progress(
            int(i / max(total_files, 1) * 100),
            text=f"Overall progress: {i}/{total_files} files",
        )
        log_box.dataframe(pd.DataFrame(results), use_container_width=True, hide_index=True)

    overall.progress(100, text="All files processed")
    return pd.DataFrame(results)


with st.sidebar:
    st.header("Input")
    mode = st.radio("Convert", ["Single parquet file", "Folder of parquet files"])

    if mode == "Single parquet file":
        parquet_path_text = st.text_input("Parquet file path", placeholder=r"D:\data\file.parquet")
        output_csv_text = st.text_input("Output CSV path", placeholder=r"D:\data\file.csv")
    else:
        folder_text = st.text_input("Parquet folder", placeholder=r"D:\data\parquet")
        recursive = st.checkbox("Scan recursively", value=True)
        output_folder_text = st.text_input("Output CSV folder", placeholder=r"D:\data\csv_output")


if pq is None:
    st.error("pyarrow is required. Install it with: pip install pyarrow")
    st.stop()


if mode == "Single parquet file":
    if not parquet_path_text:
        st.info("Enter a parquet file path in the sidebar.")
        st.stop()

    parquet_file = Path(parquet_path_text)
    if not parquet_file.exists() or parquet_file.suffix.lower() != ".parquet":
        st.error("Parquet file not found or not a .parquet file.")
        st.stop()

    output_csv = Path(output_csv_text) if output_csv_text else parquet_file.with_suffix(".csv")

    st.write("**Input:**", str(parquet_file))
    st.write("**Output:**", str(output_csv))

    if st.button("Convert to CSV", type="primary"):
        with st.spinner("Converting parquet to CSV..."):
            rows = convert_single_parquet_to_csv(parquet_file, output_csv)
        st.success(f"Done. Rows written: {rows:,}")
        st.code(str(output_csv), language="text")

else:
    if not folder_text:
        st.info("Enter a parquet folder in the sidebar.")
        st.stop()

    files = scan_parquet_files(folder_text, recursive=recursive)
    st.write(f"Found **{len(files):,}** parquet files.")

    if not files:
        st.warning("No parquet files found.")
        st.stop()

    output_folder = Path(output_folder_text) if output_folder_text else Path(folder_text) / "csv_output"

    st.write("**Output folder:**", str(output_folder))

    with st.expander("Files to convert"):
        st.dataframe(pd.DataFrame({"parquet_file": [str(f) for f in files]}), use_container_width=True, hide_index=True)

    if st.button("Convert folder to CSV", type="primary"):
        with st.spinner("Converting folder..."):
            result_df = convert_folder_to_csv(files, output_folder)

        st.success("Folder conversion complete.")
        st.dataframe(result_df, use_container_width=True, hide_index=True)

        report_path = output_folder / "conversion_report.csv"
        result_df.to_csv(report_path, index=False)
        st.info(f"Report saved: {report_path}")
