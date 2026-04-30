import os
import time
import pandas as pd
import streamlit as st
import plotly.express as px
from user_agents import parse

st.set_page_config(page_title="UA Insight Engine", layout="wide")

st.title("User-Agent CSV Insight Engine")

UPLOAD_FOLDER = r"D:\Vs - Code Work\Reports\UA"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


def find_ua_column(df):
    for col in df.columns:
        name = col.lower()
        if "user_agent" in name or "user-agent" in name or "user agent" in name:
            return col
        if name in ["ua", "agent", "useragent"]:
            return col
    return None


def find_count_column(df):
    numeric_cols = df.select_dtypes(include="number").columns.tolist()

    for col in numeric_cols:
        name = col.lower()
        if name in ["count", "requests", "hits", "total", "volume", "events"]:
            return col

    if numeric_cols:
        return numeric_cols[0]

    return None


def enrich_user_agents(df, ua_col):
    devices = []
    os_list = []
    browsers = []
    brands = []
    device_types = []
    bots = []

    for ua in df[ua_col].fillna("").astype(str):
        parsed = parse(ua)

        device = parsed.device.family or "Unknown"
        os_name = parsed.os.family or "Unknown"
        browser = parsed.browser.family or "Unknown"

        devices.append(device)
        os_list.append(os_name)
        browsers.append(browser)
        bots.append(parsed.is_bot)

        ua_lower = ua.lower()

        if "firetv" in ua_lower or "aft" in ua_lower or "amazon" in ua_lower:
            brands.append("Amazon Fire TV")
            device_types.append("Connected TV")
        elif "bravia" in ua_lower or "sony" in ua_lower:
            brands.append("Sony BRAVIA")
            device_types.append("Connected TV")
        elif "tizen" in ua_lower or "samsung" in ua_lower:
            brands.append("Samsung")
            device_types.append("Connected TV")
        elif "webos" in ua_lower or "lg" in ua_lower:
            brands.append("LG")
            device_types.append("Connected TV")
        elif "roku" in ua_lower:
            brands.append("Roku")
            device_types.append("Connected TV")
        elif "android" in ua_lower:
            brands.append("Android")
            device_types.append("Android / Mobile / CTV")
        elif "iphone" in ua_lower or "ipad" in ua_lower:
            brands.append("Apple iOS")
            device_types.append("Mobile / Tablet")
        elif "windows" in ua_lower or "macintosh" in ua_lower:
            brands.append("Desktop")
            device_types.append("Desktop")
        elif ua.strip() == "" or ua_lower in ["nan", "none", "null"]:
            brands.append("Missing / Null")
            device_types.append("Unknown")
        else:
            brands.append("Unknown")
            device_types.append("Unknown")

    df["parsed_device"] = devices
    df["parsed_os"] = os_list
    df["parsed_browser"] = browsers
    df["device_brand"] = brands
    df["device_type"] = device_types
    df["is_bot"] = bots

    return df


def weighted_value_counts(df, column, count_col=None):
    if count_col:
        return df.groupby(column)[count_col].sum().sort_values(ascending=False)
    return df[column].value_counts()


def generate_ua_insights(df, ua_col, count_col=None):
    insights = []
    flags = []

    # ----------------------------
    # Volume calculation
    # ----------------------------
    total_rows = len(df)
    total_volume = df[count_col].sum() if count_col else total_rows

    insights.append(f"Total traffic volume: {total_volume:,.0f} across {total_rows:,} unique records.")

    # ----------------------------
    # Aggregations
    # ----------------------------
    def agg(col):
        if count_col:
            return df.groupby(col)[count_col].sum().sort_values(ascending=False)
        return df[col].value_counts()

    brand = agg("device_brand")
    device_type = agg("device_type")
    os_dist = agg("parsed_os")

    # ----------------------------
    # 1. Device dominance
    # ----------------------------
    top_brand = brand.index[0]
    top_brand_share = brand.iloc[0] / total_volume

    if top_brand_share > 0.5:
        flags.append("🔥 EXTREME DEPENDENCY")
    elif top_brand_share > 0.35:
        flags.append("⚠️ HIGH DEPENDENCY")

    insights.append(
        f"{top_brand} dominates traffic with {top_brand_share:.1%} share."
    )

    # ----------------------------
    # 2. Traffic concentration (Pareto)
    # ----------------------------
    top3 = brand.head(3).sum() / total_volume
    top10 = brand.head(10).sum() / total_volume

    insights.append(f"Top 3 devices contribute {top3:.1%} of traffic.")
    insights.append(f"Top 10 devices contribute {top10:.1%} of traffic.")

    if top3 > 0.7:
        flags.append("⚠️ TRAFFIC HIGHLY CONCENTRATED")

    # ----------------------------
    # 3. Device ecosystem (CTV vs others)
    # ----------------------------
    ctv_share = device_type.get("Connected TV", 0) / total_volume
    mobile_share = device_type.get("Mobile / Tablet", 0) / total_volume
    desktop_share = device_type.get("Desktop", 0) / total_volume

    insights.append(
        f"CTV: {ctv_share:.1%}, Mobile: {mobile_share:.1%}, Desktop: {desktop_share:.1%}"
    )

    if ctv_share > 0.5:
        insights.append("Connected TV is the PRIMARY consumption platform.")
    elif mobile_share > 0.5:
        insights.append("Mobile dominates usage patterns.")

    # ----------------------------
    # 4. OS ecosystem
    # ----------------------------
    top_os = os_dist.index[0]
    top_os_share = os_dist.iloc[0] / total_volume

    insights.append(
        f"{top_os} ecosystem leads with {top_os_share:.1%} share."
    )

    if "Android" in top_os:
        insights.append("Android-based ecosystem dominates (likely OTT / CTV heavy).")

    # ----------------------------
    # 5. Bot detection
    # ----------------------------
    if count_col:
        bot_volume = df.loc[df["is_bot"] == True, count_col].sum()
    else:
        bot_volume = df["is_bot"].sum()

    bot_share = bot_volume / total_volume

    insights.append(f"Bot / tool traffic: {bot_share:.2%}")

    if bot_share > 0.05:
        flags.append("⚠️ HIGH BOT TRAFFIC")

    # ----------------------------
    # 6. Data quality
    # ----------------------------
    unknown_volume = brand.get("Unknown", 0) + brand.get("Missing / Null", 0)
    unknown_share = unknown_volume / total_volume

    if unknown_share > 0.05:
        flags.append("⚠️ DATA QUALITY ISSUE")

    insights.append(f"Unknown / unclassified traffic: {unknown_share:.1%}")

    # ----------------------------
    # 7. Long-tail analysis
    # ----------------------------
    long_tail_devices = len(brand)
    insights.append(f"{long_tail_devices:,} unique device types detected.")

    if long_tail_devices > 500:
        insights.append("Highly fragmented device ecosystem (long-tail distribution).")

    # ----------------------------
    # 8. Business recommendations
    # ----------------------------
    if top_brand_share > 0.4:
        insights.append(
            f"Optimize performance, QA, and streaming experience for {top_brand} as a priority."
        )

    if ctv_share > 0.5:
        insights.append(
            "Focus on TV UX, remote navigation, and big-screen playback optimization."
        )

    if bot_share > 0.05:
        insights.append(
            "Filter bot traffic from analytics to avoid skewed reporting."
        )

    if unknown_share > 0.05:
        insights.append(
            "Improve UA parsing or tracking to reduce unknown traffic."
        )

    # ----------------------------
    # FINAL OUTPUT
    # ----------------------------
    return flags, insights

def show_top_chart(df, column, title, count_col=None):
    data = weighted_value_counts(df, column, count_col).head(20).reset_index()
    data.columns = [column, "traffic"]

    fig = px.bar(
        data,
        x=column,
        y="traffic",
        title=title
    )

    st.plotly_chart(fig, use_container_width=True)


uploaded_file = st.file_uploader("Upload your UA CSV file", type=["csv"])

if uploaded_file:
    filename = f"{int(time.time())}_{uploaded_file.name}"
    file_path = os.path.join(UPLOAD_FOLDER, filename)

    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())

    st.success(f"File saved to: {file_path}")

    df = pd.read_csv(file_path)

    st.subheader("Raw CSV Preview")
    st.dataframe(df.head(50), use_container_width=True)

    # Your UA column name
    ua_col = "value"

    if ua_col not in df.columns:
        st.error(f"Column `{ua_col}` not found. Available columns are: {list(df.columns)}")
        st.stop()

    count_col = find_count_column(df)

    st.success(f"Detected User-Agent column: {ua_col}")

    if count_col:
        st.success(f"Detected traffic/count column: {count_col}")
    else:
        st.warning("No numeric count column detected. Each row will be treated as 1 request.")

    df = enrich_user_agents(df, ua_col)

    # ✅ FIXED PRO INSIGHT SECTION
    st.subheader("Smart UA Insights")

    flags, insights = generate_ua_insights(df, ua_col, count_col)

    if flags:
        st.subheader("🚨 Key Flags")
        for flag in flags:
            st.error(flag)

    st.subheader("🧠 Pro Insights")
    for insight in insights:
        st.info(insight)

    st.subheader("Enriched Data Preview")
    st.dataframe(df.head(100), use_container_width=True)

    st.subheader("Traffic Charts")

    col1, col2 = st.columns(2)

    with col1:
        show_top_chart(df, "device_brand", "Top Device Brands", count_col)

    with col2:
        show_top_chart(df, "device_type", "Top Device Types", count_col)

    col3, col4 = st.columns(2)

    with col3:
        show_top_chart(df, "parsed_os", "Top Operating Systems", count_col)

    with col4:
        show_top_chart(df, "parsed_browser", "Top Browsers / Clients", count_col)

    st.subheader("Bot / Tool Traffic")

    bot_data = weighted_value_counts(df, "is_bot", count_col).reset_index()
    bot_data.columns = ["is_bot", "traffic"]

    fig = px.pie(
        bot_data,
        names="is_bot",
        values="traffic",
        title="Bot vs Non-Bot Traffic"
    )

    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Download Enriched CSV")

    csv = df.to_csv(index=False).encode("utf-8")

    st.download_button(
        label="Download enriched CSV",
        data=csv,
        file_name="enriched_ua_data.csv",
        mime="text/csv"
    )

else:
    st.warning("Upload a CSV file to generate UA insights.")