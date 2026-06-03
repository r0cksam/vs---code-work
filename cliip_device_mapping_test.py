import re
import urllib.parse
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st


st.set_page_config(page_title="cliIP Device Mapping Test", layout="wide")
st.title("cliIP ↔ Device_ID Mapping Test")

folder = st.text_input("Parquet folder", placeholder=r"e.g. D:\data\parquet or /mnt/data/parquet")

if not folder:
    st.stop()

folder_path = Path(folder)
if not folder_path.is_dir():
    st.error("Folder not found.")
    st.stop()

files = sorted(folder_path.glob("*.parquet"))
if not files:
    st.error("No parquet files found.")
    st.stop()

st.success(f"Found {len(files):,} parquet files")

con = duckdb.connect()
sample_schema = con.execute(
    "DESCRIBE SELECT * FROM read_parquet(?) LIMIT 1",
    [[str(f) for f in files]],
).df()
columns = sample_schema["column_name"].tolist()

c1, c2, c3 = st.columns(3)

with c1:
    query_col = st.selectbox(
        "Query string column",
        columns,
        index=columns.index("queryStr") if "queryStr" in columns else 0,
    )

with c2:
    ip_col = st.selectbox(
        "cliIP column",
        columns,
        index=columns.index("cliIP") if "cliIP" in columns else 0,
    )

with c3:
    ts_col = st.selectbox(
        "Timestamp column",
        columns,
        index=columns.index("reqTimeSec") if "reqTimeSec" in columns else 0,
    )

date_mode = st.checkbox("Apply date filter", value=True)

if date_mode:
    min_max = con.execute(
        f"""
        SELECT
            MIN(TRY_CAST("{ts_col}" AS BIGINT)) AS min_ts,
            MAX(TRY_CAST("{ts_col}" AS BIGINT)) AS max_ts
        FROM read_parquet(?, union_by_name=true)
        WHERE TRY_CAST("{ts_col}" AS BIGINT) IS NOT NULL
        """,
        [[str(f) for f in files]],
    ).fetchone()

    min_dt = pd.to_datetime(min_max[0], unit="s").date()
    max_dt = pd.to_datetime(min_max[1], unit="s").date()

    st.caption(f"Available date range: {min_dt} → {max_dt}")

    date_range = st.date_input(
        "Date range",
        value=(min_dt, max_dt),
        min_value=min_dt,
        max_value=max_dt,
    )

    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_dt, end_dt = date_range
    else:
        start_dt, end_dt = min_dt, max_dt

    start_epoch = int(pd.Timestamp(start_dt).timestamp())
    end_epoch = int(pd.Timestamp(end_dt).replace(hour=23, minute=59, second=59).timestamp())

    date_where = f"""
    AND TRY_CAST("{ts_col}" AS BIGINT) BETWEEN {start_epoch} AND {end_epoch}
    """
else:
    date_where = ""

if st.button("Run cliIP ↔ device_id test", type="primary"):
    parquet_files = [str(f) for f in files]

    base_query = f"""
    WITH base AS (
        SELECT
            CAST("{ip_col}" AS VARCHAR) AS cliIP,
            regexp_extract(CAST("{query_col}" AS VARCHAR), '(?:^|&)device_id=([^&]+)', 1) AS device_id,
            TRY_CAST("{ts_col}" AS BIGINT) AS reqTimeSec
        FROM read_parquet(?, union_by_name=true)
        WHERE "{query_col}" IS NOT NULL
          AND "{ip_col}" IS NOT NULL
          AND TRIM(CAST("{ip_col}" AS VARCHAR)) <> ''
          {date_where}
    ),
    clean AS (
        SELECT
            cliIP,
            url_decode(device_id) AS device_id,
            reqTimeSec
        FROM base
        WHERE device_id IS NOT NULL
          AND TRIM(device_id) <> ''
    )
    SELECT * FROM clean
    """

    with st.spinner("Loading cliIP/device_id pairs..."):
        df = con.execute(base_query, [parquet_files]).df()

    if df.empty:
        st.warning("No rows found where both cliIP and device_id are available.")
        st.stop()

    total_rows = len(df)
    total_ips = df["cliIP"].nunique()
    total_devices = df["device_id"].nunique()

    k1, k2, k3 = st.columns(3)
    k1.metric("Rows with both cliIP + device_id", f"{total_rows:,}")
    k2.metric("Distinct cliIP", f"{total_ips:,}")
    k3.metric("Distinct device_id", f"{total_devices:,}")

    st.markdown("---")

    ip_summary = (
        df.groupby("cliIP")
        .agg(
            rows=("device_id", "size"),
            distinct_devices=("device_id", "nunique"),
            first_seen=("reqTimeSec", "min"),
            last_seen=("reqTimeSec", "max"),
        )
        .reset_index()
    )

    top_device_per_ip = (
        df.groupby(["cliIP", "device_id"])
        .size()
        .reset_index(name="device_rows")
        .sort_values(["cliIP", "device_rows"], ascending=[True, False])
    )

    top_device_only = top_device_per_ip.drop_duplicates("cliIP")
    ip_summary = ip_summary.merge(
        top_device_only[["cliIP", "device_id", "device_rows"]],
        on="cliIP",
        how="left",
    )

    ip_summary = ip_summary.rename(columns={"device_id": "top_device_id"})
    ip_summary["top_device_row_pct"] = (
        ip_summary["device_rows"] / ip_summary["rows"] * 100
    ).round(2)

    ip_summary["first_seen"] = pd.to_datetime(ip_summary["first_seen"], unit="s", errors="coerce")
    ip_summary["last_seen"] = pd.to_datetime(ip_summary["last_seen"], unit="s", errors="coerce")

    ip_summary = ip_summary.sort_values(
        ["distinct_devices", "rows"],
        ascending=[False, False],
    )

    st.subheader("1) cliIP → device_id mapping")
    st.caption("If most cliIP values have distinct_devices = 1, cliIP is a reasonable fallback. If many have 10, 50, 100+ devices, cliIP is shared and risky.")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("IPs with 1 device", f"{(ip_summary['distinct_devices'] == 1).sum():,}")
    c2.metric("IPs with 2-5 devices", f"{((ip_summary['distinct_devices'] >= 2) & (ip_summary['distinct_devices'] <= 5)).sum():,}")
    c3.metric("IPs with 6-20 devices", f"{((ip_summary['distinct_devices'] >= 6) & (ip_summary['distinct_devices'] <= 20)).sum():,}")
    c4.metric("IPs with 20+ devices", f"{(ip_summary['distinct_devices'] > 20).sum():,}")

    st.dataframe(ip_summary, use_container_width=True, hide_index=True, height=420)

    st.download_button(
        "Download cliIP to device_id summary CSV",
        data=ip_summary.to_csv(index=False).encode("utf-8"),
        file_name="cliip_to_deviceid_summary.csv",
        mime="text/csv",
    )

    st.markdown("---")

    device_summary = (
        df.groupby("device_id")
        .agg(
            rows=("cliIP", "size"),
            distinct_ips=("cliIP", "nunique"),
            first_seen=("reqTimeSec", "min"),
            last_seen=("reqTimeSec", "max"),
        )
        .reset_index()
    )

    top_ip_per_device = (
        df.groupby(["device_id", "cliIP"])
        .size()
        .reset_index(name="ip_rows")
        .sort_values(["device_id", "ip_rows"], ascending=[True, False])
    )

    top_ip_only = top_ip_per_device.drop_duplicates("device_id")
    device_summary = device_summary.merge(
        top_ip_only[["device_id", "cliIP", "ip_rows"]],
        on="device_id",
        how="left",
    )

    device_summary = device_summary.rename(columns={"cliIP": "top_cliIP"})
    device_summary["top_ip_row_pct"] = (
        device_summary["ip_rows"] / device_summary["rows"] * 100
    ).round(2)

    device_summary["first_seen"] = pd.to_datetime(device_summary["first_seen"], unit="s", errors="coerce")
    device_summary["last_seen"] = pd.to_datetime(device_summary["last_seen"], unit="s", errors="coerce")

    device_summary = device_summary.sort_values(
        ["distinct_ips", "rows"],
        ascending=[False, False],
    )

    st.subheader("2) device_id → cliIP mapping")
    st.caption("This shows whether one device keeps changing IPs. That is normal on mobile networks, but useful to know.")

    d1, d2, d3, d4 = st.columns(4)
    d1.metric("Devices with 1 IP", f"{(device_summary['distinct_ips'] == 1).sum():,}")
    d2.metric("Devices with 2-5 IPs", f"{((device_summary['distinct_ips'] >= 2) & (device_summary['distinct_ips'] <= 5)).sum():,}")
    d3.metric("Devices with 6-20 IPs", f"{((device_summary['distinct_ips'] >= 6) & (device_summary['distinct_ips'] <= 20)).sum():,}")
    d4.metric("Devices with 20+ IPs", f"{(device_summary['distinct_ips'] > 20).sum():,}")

    st.dataframe(device_summary, use_container_width=True, hide_index=True, height=420)

    st.download_button(
        "Download device_id to cliIP summary CSV",
        data=device_summary.to_csv(index=False).encode("utf-8"),
        file_name="deviceid_to_cliip_summary.csv",
        mime="text/csv",
    )

    st.markdown("---")

    st.subheader("3) Final recommendation helper")

    ip_one_device_pct = (ip_summary["distinct_devices"].eq(1).sum() / len(ip_summary) * 100) if len(ip_summary) else 0
    row_on_single_device_ip_pct = (
        ip_summary.loc[ip_summary["distinct_devices"].eq(1), "rows"].sum()
        / ip_summary["rows"].sum()
        * 100
    ) if ip_summary["rows"].sum() else 0

    st.write(f"**% of cliIP values mapping to exactly 1 device:** {ip_one_device_pct:.2f}%")
    st.write(f"**% of rows on cliIP values that map to exactly 1 device:** {row_on_single_device_ip_pct:.2f}%")

    if row_on_single_device_ip_pct >= 80:
        st.success("cliIP fallback looks reasonably safe for most rows.")
    elif row_on_single_device_ip_pct >= 50:
        st.warning("cliIP fallback is usable, but shared-IP distortion may be meaningful.")
    else:
        st.error("cliIP fallback is risky. Many rows are on shared IPs with multiple device IDs.")

        