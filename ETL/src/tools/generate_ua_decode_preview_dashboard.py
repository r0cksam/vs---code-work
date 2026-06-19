#!/usr/bin/env python3
"""Generate a static UA/device intelligence preview dashboard.

This dashboard is a read-only snapshot over:
- ETL/distinct_UA_Both_All.csv
- ETL/data/cache/device_decode/whatmyuseragent_distinct_ua_cache.parquet

It writes a separate HTML preview and does not touch the long-running API
decode process or its final lookup files.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

import decode_distinct_ua_lookup as decoder


ETL_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT = ETL_ROOT / "distinct_UA_Both_All.csv"
DEFAULT_MAP = ETL_ROOT / "config" / "device_decode" / "amazon_fire_tv_models.csv"
DEFAULT_API_CACHE = ETL_ROOT / "data" / "cache" / "device_decode" / "whatmyuseragent_distinct_ua_cache.parquet"
DEFAULT_OUT = ETL_ROOT / "output" / "device_decode" / "ua_decode_dashboard_preview.html"
DEFAULT_LOG = ETL_ROOT / "output" / "logs" / "ua_decode" / "decode_distinct_ua_all_20260618_125918.out.log"


def read_api_cache_safely(path: Path, attempts: int = 4) -> pd.DataFrame:
    """Read cache while the API process may be atomically replacing it."""
    last_error: Exception | None = None
    for _ in range(attempts):
        try:
            return decoder.load_api_cache(path)
        except Exception as exc:  # noqa: BLE001 - preview should degrade gracefully.
            last_error = exc
            time.sleep(0.5)
    if last_error:
        print(f"WARNING: could not read API cache: {last_error}")
    return pd.DataFrame(columns=decoder.API_COLUMNS)


def latest_api_progress(log_path: Path) -> tuple[int, int, str]:
    if not log_path.exists():
        return 0, 0, ""
    latest = (0, 0, "")
    for line in log_path.read_text(encoding="utf-8", errors="replace").splitlines():
        if " API " not in line or "/" not in line:
            continue
        try:
            part = line.split(" API ", 1)[1].split(" ", 1)[0]
            done, total = part.split("/", 1)
            latest = (int(done), int(total), line)
        except (ValueError, IndexError):
            continue
    return latest


def pct(part: int, total: int) -> float:
    return round((part / max(total, 1)) * 100, 2)


def top_records(frame: pd.DataFrame, group_col: str, limit: int = 12) -> list[dict[str, Any]]:
    if group_col not in frame.columns:
        return []
    grouped = (
        frame.assign(_value=frame[group_col].fillna("").replace("", "Unknown / NA"))
        .groupby("_value", dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(["count", "_value"], ascending=[False, True])
        .head(limit)
    )
    total = int(len(frame))
    return [
        {"label": str(row["_value"]), "count": int(row["count"]), "share": pct(int(row["count"]), total)}
        for _, row in grouped.iterrows()
    ]


def top_combo_records(frame: pd.DataFrame, cols: list[str], label: str, limit: int = 14) -> list[dict[str, Any]]:
    available = [col for col in cols if col in frame.columns]
    if not available:
        return []
    tmp = frame.copy()
    tmp["_combo"] = tmp[available].fillna("").astype(str).apply(
        lambda row: " / ".join([item for item in row if item.strip()]) or "Unknown / NA",
        axis=1,
    )
    out = (
        tmp.groupby("_combo", dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(["count", "_combo"], ascending=[False, True])
        .head(limit)
    )
    total = int(len(frame))
    return [
        {"label": str(row["_combo"]), "count": int(row["count"]), "share": pct(int(row["count"]), total), "kind": label}
        for _, row in out.iterrows()
    ]


def table_records_from_counts(
    frame: pd.DataFrame,
    group_cols: list[str],
    limit: int = 18,
) -> list[dict[str, Any]]:
    available = [column for column in group_cols if column in frame.columns]
    if not available or frame.empty:
        return []
    grouped = (
        frame.groupby(available, dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(["count", *available], ascending=[False, *([True] * len(available))])
        .head(limit)
    )
    total = int(len(frame))
    rows: list[dict[str, Any]] = []
    for _, row in grouped.iterrows():
        item = {column: str(row[column]) for column in available}
        item["count"] = int(row["count"])
        item["share"] = pct(int(row["count"]), total)
        rows.append(item)
    return rows


def sample_rows(frame: pd.DataFrame, columns: list[str], limit: int = 16) -> list[dict[str, str]]:
    available = [column for column in columns if column in frame.columns]
    rows: list[dict[str, str]] = []
    for _, row in frame.head(limit).iterrows():
        rows.append({column: str(row.get(column, "")) for column in available})
    return rows


def build_decoded(input_csv: Path, map_path: Path, api_cache_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    distinct = decoder.read_distinct_ua_csv(input_csv, None)
    fire_map = decoder.load_fire_tv_map(map_path)
    local = pd.DataFrame([decoder.local_decode_row(row, fire_map) for _, row in distinct.iterrows()])
    api_cache = read_api_cache_safely(api_cache_path)
    decoded = decoder.apply_api_to_local(local, api_cache)
    for column in decoder.OUTPUT_COLUMNS:
        if column not in decoded.columns:
            decoded[column] = ""
    return decoded[decoder.OUTPUT_COLUMNS], api_cache


def extract_app_name(ua: str) -> str:
    text = str(ua or "").strip()
    match = re.match(r"([A-Za-z][A-Za-z0-9_.+-]{1,60})/[0-9A-Za-z_.+-]+", text)
    if match:
        return match.group(1)
    if "m3u-ip.tv" in text:
        return "m3u-ip.tv"
    if "iptv" in text.lower():
        return "IPTV app/checker"
    return ""


def has_value(frame: pd.DataFrame, columns: list[str]) -> pd.Series:
    available = [column for column in columns if column in frame.columns]
    if not available:
        return pd.Series(False, index=frame.index)
    return frame[available].fillna("").astype(str).apply(lambda row: any(item.strip() for item in row), axis=1)


def clean_text(value: Any) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "null"} else text


def classify_unknown_pattern(row: pd.Series) -> str:
    ua = clean_text(row.get("ua_norm", ""))
    low = ua.lower()
    if not ua:
        return "Blank UA"
    if "akamai-test" in low:
        return "Akamai test traffic"
    if "iptv" in low or "m3u" in low:
        return "IPTV / playlist tooling"
    if "tivimate" in low or "ott navigator" in low or "streamvault" in low:
        return "Player app without device identity"
    if "android" in low and ("build/" in low or "build;" in low):
        return "Android UA missing mapped model"
    if "libvlc" in low or "ffmpeg" in low or "exoplayer" in low or "hls" in low:
        return "Media library / player"
    if len(ua) <= 18:
        return "Short token UA"
    if re.search(r"%[0-9a-fA-F]{2}|[;)(]{2,}|[A-Za-z0-9+/]{28,}", ua):
        return "Encoded or noisy UA"
    return "Needs manual review"


def api_detail_level(row: pd.Series) -> str:
    brand = clean_text(row.get("api_brand", ""))
    model = clean_text(row.get("api_model", ""))
    device = clean_text(row.get("api_device_type", ""))
    os_name = clean_text(row.get("api_os_name", ""))
    browser = clean_text(row.get("api_browser_name", ""))
    if brand and model:
        return "Brand + model"
    if brand or model:
        return "Brand/model partial"
    if device:
        return "Device type only"
    if os_name or browser:
        return "OS/browser only"
    return "No useful API detail"


def add_derived_fields(decoded: pd.DataFrame) -> pd.DataFrame:
    out = decoded.copy()
    out["brand_display"] = out["brand"].fillna("").replace("", "Unknown / NA")
    out["model_display"] = out["model"].fillna("").replace("", "Unknown / NA")
    out["os_display"] = (
        out["os_name"].fillna("").astype(str)
        + " "
        + out["os_version"].fillna("").astype(str)
    ).str.strip().replace("", "Unknown / NA")
    out["browser_display"] = (
        out["browser_name"].fillna("").astype(str)
        + " "
        + out["browser_version"].fillna("").astype(str)
    ).str.strip().replace("", "Unknown / NA")
    out["app_name"] = out["ua_norm"].map(extract_app_name).replace("", "Unknown / NA")
    out["api_has_device_detail"] = (
        out[["api_device_type", "api_brand", "api_model", "api_os_name", "api_browser_name"]]
        .fillna("")
        .astype(str)
        .apply(lambda row: any(item.strip() for item in row), axis=1)
    )
    out["identity_known"] = has_value(out, ["brand", "model"])
    out["api_identity_known"] = has_value(out, ["api_brand", "api_model"])
    out["api_detail_level"] = out.apply(api_detail_level, axis=1)
    out["unknown_pattern"] = out.apply(classify_unknown_pattern, axis=1)
    out["brand_os_combo"] = out["brand_display"] + " / " + out["os_display"]
    out["brand_model_combo"] = out["brand_display"] + " / " + out["model_display"]
    out["device_identity"] = out[["form_factor", "brand_display", "model_display"]].astype(str).agg(" / ".join, axis=1)
    return out


def html_doc(data: dict[str, Any]) -> str:
    payload = json.dumps(data, ensure_ascii=False).replace("</", "<\\/")
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Veto UA Device Intelligence Preview</title>
<style>
:root {{
  --bg:#f6f8fb; --panel:#fff; --text:#172033; --muted:#64748b; --line:#dde5ef;
  --blue:#2563eb; --green:#059669; --amber:#d97706; --red:#dc2626; --violet:#7c3aed;
  --cyan:#0891b2; --ink:#0f172a;
}}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--bg); color:var(--text); font-family:"Segoe UI",Arial,sans-serif; }}
html,body {{ max-width:100%; overflow-x:hidden; }}
header {{ position:sticky; top:0; z-index:5; background:rgba(255,255,255,.96); border-bottom:1px solid var(--line); backdrop-filter:blur(8px); }}
.wrap {{ width:min(1540px, calc(100vw - 32px)); margin:0 auto; max-width:100%; min-width:0; }}
.head {{ display:flex; align-items:center; justify-content:space-between; gap:16px; padding:14px 0; }}
h1 {{ margin:0; font-size:22px; line-height:1.2; letter-spacing:0; }}
.sub {{ color:var(--muted); font-size:13px; margin-top:4px; }}
.pillrow {{ display:flex; flex-wrap:wrap; gap:8px; justify-content:flex-end; }}
.pill {{ border:1px solid var(--line); background:#f8fafc; border-radius:999px; padding:7px 10px; font-size:12px; color:var(--muted); }}
main {{ padding:18px 0 34px; }}
.section {{ margin-bottom:18px; scroll-margin-top:90px; }}
.title {{ display:flex; align-items:end; justify-content:space-between; gap:12px; margin-bottom:10px; }}
.title h2 {{ margin:0; font-size:17px; }}
.title span {{ color:var(--muted); font-size:12px; text-align:right; }}
.grid {{ display:grid; gap:10px; }}
.kpis {{ grid-template-columns:repeat(auto-fit, minmax(175px, 1fr)); }}
.two {{ grid-template-columns:repeat(auto-fit, minmax(410px, 1fr)); }}
.three {{ grid-template-columns:repeat(auto-fit, minmax(310px, 1fr)); }}
.card {{ background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:13px; min-width:0; box-shadow:0 1px 2px rgba(15,23,42,.04); }}
.metric-label {{ color:var(--muted); font-size:12px; }}
.metric-value {{ font-size:25px; font-weight:700; margin-top:6px; line-height:1.1; overflow-wrap:anywhere; }}
.metric-note {{ color:var(--muted); font-size:12px; margin-top:6px; }}
.barlist {{ display:grid; gap:9px; }}
.barrow {{ display:grid; grid-template-columns:minmax(0, 230px) minmax(0,1fr) minmax(54px,auto); gap:10px; align-items:center; font-size:13px; max-width:100%; min-width:0; }}
.barlabel {{ overflow-wrap:anywhere; color:#263348; }}
.track {{ height:8px; border-radius:999px; background:#eef2f7; overflow:hidden; }}
.fill {{ height:100%; border-radius:999px; min-width:2px; }}
.num {{ color:var(--muted); font-variant-numeric:tabular-nums; text-align:right; }}
.tablewrap {{ overflow-x:hidden; overflow-y:auto; max-width:100%; max-height:460px; border:1px solid var(--line); border-radius:8px; }}
table {{ width:100%; min-width:0; table-layout:fixed; border-collapse:collapse; font-size:12px; background:#fff; }}
th,td {{ padding:9px 10px; border-bottom:1px solid #edf1f7; text-align:left; vertical-align:top; }}
th {{ color:#475569; font-weight:650; background:#f8fafc; position:sticky; top:0; z-index:1; }}
th,td {{ white-space:normal; overflow-wrap:anywhere; word-break:break-word; }}
td {{ color:#1f2937; }}
.mono {{ font-family:Consolas,"Courier New",monospace; overflow-wrap:anywhere; min-width:0; }}
.controls {{ display:flex; gap:10px; flex-wrap:wrap; margin-bottom:10px; }}
.controls input,.controls select {{ border:1px solid var(--line); background:#fff; border-radius:6px; padding:8px 10px; font:inherit; font-size:13px; }}
.controls input {{ min-width:0; flex:1 1 220px; max-width:100%; }}
.note {{ color:var(--muted); font-size:12px; line-height:1.45; }}
.tag {{ display:inline-block; border:1px solid var(--line); background:#f8fafc; border-radius:999px; padding:3px 7px; color:#334155; font-size:11px; }}
@media (max-width:760px) {{
  .wrap {{ width:min(100vw - 20px, 1540px); }}
  .head {{ align-items:flex-start; flex-direction:column; }}
  .pillrow {{ justify-content:flex-start; }}
  .two,.three {{ grid-template-columns:1fr; }}
  .barrow {{ grid-template-columns:1fr; gap:5px; }}
}}
</style>
</head>
<body>
<header>
  <div class="wrap head">
    <div>
      <h1>Veto UA Device Intelligence Preview</h1>
      <div class="sub">Current distinct-UA decode snapshot. This is not usage-weighted yet; it shows what device intelligence is available from UA parsing so far.</div>
    </div>
    <div class="pillrow" id="topPills"></div>
  </div>
</header>
<main class="wrap">
  <section class="section">
    <div class="title"><h2>Decode Coverage</h2><span id="coverageNote"></span></div>
    <div class="grid kpis" id="kpis"></div>
  </section>
  <section class="section">
    <div class="title"><h2>Insight Summary</h2><span>actionable interpretation from current decode state</span></div>
    <div class="grid kpis" id="insightCards"></div>
  </section>
  <section class="section">
    <div class="card">
      <div class="title"><h2>Dashboard Readiness</h2><span>what can safely power stakeholder dashboards now</span></div>
      <div class="tablewrap" id="readinessTable"></div>
    </div>
  </section>
  <section class="section">
    <div class="grid two">
      <div class="card"><div class="title"><h2>Form Factor</h2><span>CTV / mobile / desktop split</span></div><div class="barlist" id="formFactorBars"></div></div>
      <div class="card"><div class="title"><h2>Decode Status</h2><span>local + API result state</span></div><div class="barlist" id="statusBars"></div></div>
    </div>
  </section>
  <section class="section">
    <div class="grid three">
      <div class="card"><div class="title"><h2>Device Type</h2><span>decoded category</span></div><div class="barlist" id="deviceTypeBars"></div></div>
      <div class="card"><div class="title"><h2>Top Brands</h2><span>decoded brand</span></div><div class="barlist" id="brandBars"></div></div>
      <div class="card"><div class="title"><h2>Top Models</h2><span>device model / family</span></div><div class="barlist" id="modelBars"></div></div>
    </div>
  </section>
  <section class="section">
    <div class="grid three">
      <div class="card"><div class="title"><h2>OS Detail</h2><span>name + version</span></div><div class="barlist" id="osDetailBars"></div></div>
      <div class="card"><div class="title"><h2>Browser Detail</h2><span>browser + version</span></div><div class="barlist" id="browserBars"></div></div>
      <div class="card"><div class="title"><h2>App / Player</h2><span>detected in UA</span></div><div class="barlist" id="playerBars"></div></div>
    </div>
  </section>
  <section class="section">
    <div class="title"><h2>CTV Deep Dive</h2><span>connected-TV insight from decoded UA fields</span></div>
    <div class="grid three">
      <div class="card"><div class="title"><h2>CTV Brands</h2><span>brand distribution</span></div><div class="barlist" id="ctvBrandBars"></div></div>
      <div class="card"><div class="title"><h2>CTV Models</h2><span>model distribution</span></div><div class="barlist" id="ctvModelBars"></div></div>
      <div class="card"><div class="title"><h2>CTV OS</h2><span>OS and version</span></div><div class="barlist" id="ctvOsBars"></div></div>
    </div>
  </section>
  <section class="section">
    <div class="grid two">
      <div class="card"><div class="title"><h2>API Detail Quality</h2><span>separates useful decode from broad labels</span></div><div class="barlist" id="apiQualityBars"></div></div>
      <div class="card"><div class="title"><h2>Unknown Pattern Buckets</h2><span>where remaining unknowns should be handled</span></div><div class="barlist" id="unknownPatternBars"></div></div>
    </div>
  </section>
  <section class="section">
    <div class="grid two">
      <div class="card"><div class="title"><h2>Brand / OS Matrix</h2><span>decoded combinations</span></div><div class="tablewrap" id="brandOsTable"></div></div>
      <div class="card"><div class="title"><h2>Device Identity Families</h2><span>form factor + brand + model</span></div><div class="tablewrap" id="identityTable"></div></div>
    </div>
  </section>
  <section class="section">
    <div class="grid two">
      <div class="card"><div class="title"><h2>Fire TV / Model Codes</h2><span>mapped local evidence</span></div><div class="tablewrap" id="modelCodeTable"></div></div>
      <div class="card"><div class="title"><h2>API Enriched Samples</h2><span>WhatMyUserAgent fields now cached</span></div><div class="tablewrap" id="apiTable"></div></div>
    </div>
  </section>
  <section class="section">
    <div class="card">
      <div class="title"><h2>Decoded UA Explorer</h2><span>search current snapshot</span></div>
      <div class="controls">
        <input id="searchInput" type="search" placeholder="Search UA, brand, model, OS, browser, app..."/>
        <select id="statusFilter"><option value="all">All statuses</option></select>
        <select id="formFilter"><option value="all">All form factors</option></select>
        <select id="limitSelect"><option>25</option><option selected>50</option><option>100</option><option>200</option></select>
      </div>
      <div class="tablewrap" id="explorerTable"></div>
      <div class="note" id="explorerNote"></div>
    </div>
  </section>
  <section class="section">
    <div class="grid two">
      <div class="card"><div class="title"><h2>Unknown Review</h2><span>still needs rules/API/manual mapping</span></div><div class="tablewrap" id="unknownTable"></div></div>
      <div class="card"><div class="title"><h2>Malformed / Noise</h2><span>payloads and bad UA shapes</span></div><div class="tablewrap" id="malformedTable"></div></div>
    </div>
  </section>
</main>
<script>
const DATA = {payload};
const fmt = new Intl.NumberFormat('en-IN');
const COLORS = ['#2563eb','#059669','#d97706','#7c3aed','#0891b2','#dc2626','#475569','#16a34a','#ea580c','#9333ea'];
let explorerState = {{search:'', status:'all', form:'all', limit:50}};
function pct(v,total) {{ return total ? ((v/total)*100).toFixed(1)+'%' : '0.0%'; }}
function esc(v) {{ return String(v ?? '').replace(/[&<>"']/g, s => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[s])); }}
function color(i) {{ return COLORS[i % COLORS.length]; }}
function pills() {{
  const p = [
    ['Generated', DATA.generated_at],
    ['API Progress', `${{DATA.api_progress.done}} / ${{DATA.api_progress.total || 0}}`],
    ['API Cache', fmt.format(DATA.stats.api_cache_rows)],
    ['Input UA', fmt.format(DATA.stats.total)]
  ];
  document.getElementById('topPills').innerHTML = p.map(x=>`<span class="pill">${{esc(x[0])}}: <b>${{esc(x[1])}}</b></span>`).join('');
}}
function kpis() {{
  const s = DATA.stats;
  const items = [
    ['Total Distinct UA', s.total, 'deduplicated by normalized UA hash'],
    ['Decoded Local', s.local_decoded, pct(s.local_decoded, s.total)],
    ['Decoded API', s.api_decoded, 'WhatMyUserAgent cache applied'],
    ['Unknown', s.unknown, pct(s.unknown, s.total)],
    ['Malformed', s.malformed, pct(s.malformed, s.total)],
    ['CTV UA', s.ctv, pct(s.ctv, s.total)],
    ['Brand Known', s.brand_known, pct(s.brand_known, s.total)],
    ['Model Known', s.model_known, pct(s.model_known, s.total)]
  ];
  document.getElementById('kpis').innerHTML = items.map(x=>`<div class="card"><div class="metric-label">${{esc(x[0])}}</div><div class="metric-value">${{fmt.format(x[1])}}</div><div class="metric-note">${{esc(x[2])}}</div></div>`).join('');
  document.getElementById('coverageNote').textContent = `${{pct(s.local_decoded + s.api_decoded, s.total)}} decoded by distinct UA count`;
}}
function insightCards() {{
  const items = DATA.insight_cards || [];
  document.getElementById('insightCards').innerHTML = items.map(x=>`<div class="card"><div class="metric-label">${{esc(x.label)}}</div><div class="metric-value">${{esc(x.value)}}</div><div class="metric-note">${{esc(x.note)}}</div></div>`).join('');
}}
function bars(id, rows) {{
  const max = Math.max(...rows.map(r=>r.count), 1);
  document.getElementById(id).innerHTML = rows.map((r,i)=>`<div class="barrow"><div class="barlabel" title="${{esc(r.label)}}">${{esc(r.label)}}</div><div class="track"><div class="fill" style="width:${{Math.max(1,(r.count/max)*100)}}%;background:${{color(i)}}"></div></div><div class="num">${{fmt.format(r.count)}} | ${{r.share}}%</div></div>`).join('');
}}
function table(id, rows, cols) {{
  if(!rows.length) {{ document.getElementById(id).innerHTML='<table><tbody><tr><td>No rows yet</td></tr></tbody></table>'; return; }}
  const head = cols.map(c=>`<th>${{esc(c.label)}}</th>`).join('');
  const body = rows.map(r=>`<tr>${{cols.map(c=>`<td class="${{c.mono?'mono':''}}">${{esc(r[c.key] || '')}}</td>`).join('')}}</tr>`).join('');
  document.getElementById(id).innerHTML = `<table><thead><tr>${{head}}</tr></thead><tbody>${{body}}</tbody></table>`;
}}
function populateFilters() {{
  const statuses = [...new Set(DATA.explorer.map(r=>r.decode_status || 'Unknown'))].sort();
  const forms = [...new Set(DATA.explorer.map(r=>r.form_factor || 'Unknown'))].sort();
  document.getElementById('statusFilter').innerHTML = '<option value="all">All statuses</option>' + statuses.map(x=>`<option value="${{esc(x)}}">${{esc(x)}}</option>`).join('');
  document.getElementById('formFilter').innerHTML = '<option value="all">All form factors</option>' + forms.map(x=>`<option value="${{esc(x)}}">${{esc(x)}}</option>`).join('');
}}
function renderExplorer() {{
  const q = explorerState.search.toLowerCase();
  let rows = DATA.explorer.filter(r => {{
    if(explorerState.status !== 'all' && r.decode_status !== explorerState.status) return false;
    if(explorerState.form !== 'all' && r.form_factor !== explorerState.form) return false;
    if(q) {{
      const hay = [r.ua_norm,r.brand,r.model,r.os_display,r.browser_display,r.app_name,r.api_brand,r.api_model].join(' ').toLowerCase();
      if(!hay.includes(q)) return false;
    }}
    return true;
  }});
  const total = rows.length;
  rows = rows.slice(0, explorerState.limit);
  table('explorerTable', rows, [
    {{key:'decode_status', label:'Status'}}, {{key:'form_factor', label:'Form'}}, {{key:'device_type', label:'Device'}},
    {{key:'brand', label:'Brand'}}, {{key:'model', label:'Model'}}, {{key:'os_display', label:'OS'}},
    {{key:'browser_display', label:'Browser'}}, {{key:'app_name', label:'App'}}, {{key:'ua_norm', label:'UA', mono:true}}
  ]);
  document.getElementById('explorerNote').textContent = `Showing ${{fmt.format(rows.length)}} of ${{fmt.format(total)}} matching decoded UA rows.`;
}}
function bind() {{
  document.getElementById('searchInput').addEventListener('input', e=>{{ explorerState.search=e.target.value; renderExplorer(); }});
  document.getElementById('statusFilter').addEventListener('change', e=>{{ explorerState.status=e.target.value; renderExplorer(); }});
  document.getElementById('formFilter').addEventListener('change', e=>{{ explorerState.form=e.target.value; renderExplorer(); }});
  document.getElementById('limitSelect').addEventListener('change', e=>{{ explorerState.limit=Number(e.target.value); renderExplorer(); }});
}}
pills(); kpis(); insightCards(); populateFilters(); bind();
bars('formFactorBars', DATA.form_factor);
bars('statusBars', DATA.decode_status);
bars('deviceTypeBars', DATA.device_type);
bars('brandBars', DATA.brands);
bars('modelBars', DATA.models);
bars('osDetailBars', DATA.os_detail);
bars('browserBars', DATA.browser_detail);
bars('playerBars', DATA.app_player);
bars('ctvBrandBars', DATA.ctv_brands);
bars('ctvModelBars', DATA.ctv_models);
bars('ctvOsBars', DATA.ctv_os);
bars('apiQualityBars', DATA.api_detail_quality);
bars('unknownPatternBars', DATA.unknown_patterns);
table('readinessTable', DATA.dashboard_readiness, [
  {{key:'field', label:'Field'}}, {{key:'status', label:'Status'}}, {{key:'coverage', label:'Coverage'}}, {{key:'use', label:'Dashboard Use'}}, {{key:'caution', label:'Caution'}}
]);
table('brandOsTable', DATA.brand_os_matrix, [
  {{key:'brand_display', label:'Brand'}}, {{key:'os_display', label:'OS'}}, {{key:'count', label:'UA Count'}}, {{key:'share', label:'Share'}}
]);
table('identityTable', DATA.identity_families, [
  {{key:'form_factor', label:'Form'}}, {{key:'brand_display', label:'Brand'}}, {{key:'model_display', label:'Model'}}, {{key:'count', label:'UA Count'}}, {{key:'share', label:'Share'}}
]);
table('modelCodeTable', DATA.model_codes, [
  {{key:'model_code', label:'Code'}}, {{key:'brand', label:'Brand'}}, {{key:'model', label:'Model'}}, {{key:'product_family', label:'Family'}},
  {{key:'generation', label:'Gen'}}, {{key:'count', label:'UA Count'}}, {{key:'ua_norm', label:'Sample UA', mono:true}}
]);
table('apiTable', DATA.api_samples, [
  {{key:'api_device_type', label:'API Device'}}, {{key:'api_brand', label:'API Brand'}}, {{key:'api_model', label:'API Model'}},
  {{key:'api_os_name', label:'API OS'}}, {{key:'api_browser_name', label:'API Browser'}}, {{key:'ua_norm', label:'UA', mono:true}}
]);
table('unknownTable', DATA.unknown_samples, [
  {{key:'form_factor', label:'Form'}}, {{key:'os_display', label:'OS'}}, {{key:'app_name', label:'App'}}, {{key:'notes', label:'Notes'}}, {{key:'ua_norm', label:'UA', mono:true}}
]);
table('malformedTable', DATA.malformed_samples, [
  {{key:'notes', label:'Reason'}}, {{key:'ua_norm', label:'UA', mono:true}}
]);
renderExplorer();
</script>
</body>
</html>
"""


def build_payload(decoded: pd.DataFrame, api_cache: pd.DataFrame, api_progress: tuple[int, int, str]) -> dict[str, Any]:
    df = add_derived_fields(decoded)
    total = int(len(df))
    done, api_total, latest_line = api_progress
    local_decoded = int((df["decode_status"] == "decoded_local").sum())
    api_decoded = int((df["decode_status"] == "decoded_api").sum())
    stats = {
        "total": total,
        "local_decoded": local_decoded,
        "api_decoded": api_decoded,
        "unknown": int((df["decode_status"] == "unknown").sum()),
        "malformed": int((df["decode_status"] == "malformed").sum()),
        "ctv": int((df["form_factor"] == "CTV").sum()),
        "brand_known": int((df["brand"].fillna("").astype(str).str.strip() != "").sum()),
        "model_known": int((df["model"].fillna("").astype(str).str.strip() != "").sum()),
        "identity_known": int(df["identity_known"].sum()),
        "api_useful_detail": int(df["api_has_device_detail"].sum()),
        "api_identity_known": int(df["api_identity_known"].sum()),
        "api_cache_rows": int(len(api_cache)),
    }
    ctv = df[df["form_factor"].eq("CTV")].copy()
    ctv_identity_known = int(ctv["identity_known"].sum()) if not ctv.empty else 0
    api_no_useful_detail = int((df["decode_status"].eq("decoded_api") & ~df["api_has_device_detail"]).sum())
    unknown_actionable = int(df["decode_status"].eq("unknown").sum())

    insight_cards = [
        {
            "label": "Usable Device Identity",
            "value": f"{stats['identity_known']:,}",
            "note": f"{pct(stats['identity_known'], total)}% of distinct UA have brand or model.",
        },
        {
            "label": "CTV Identity Coverage",
            "value": f"{ctv_identity_known:,}",
            "note": f"{pct(ctv_identity_known, max(int(len(ctv)), 1))}% of CTV UA have brand/model.",
        },
        {
            "label": "Useful API Enrichment",
            "value": f"{stats['api_useful_detail']:,}",
            "note": "API rows where device, brand, model, OS, or browser was populated.",
        },
        {
            "label": "API Broad/Blank Results",
            "value": f"{api_no_useful_detail:,}",
            "note": "API touched these rows but did not add enough device detail for dashboard use.",
        },
        {
            "label": "Unknown To Review",
            "value": f"{unknown_actionable:,}",
            "note": "Remaining UAs grouped below into pattern buckets for next rules.",
        },
        {
            "label": "Current API Progress",
            "value": f"{done:,} / {api_total:,}",
            "note": "Live background decode progress from the log file.",
        },
    ]

    dashboard_readiness = [
        {
            "field": "Form factor",
            "status": "Ready",
            "coverage": f"{pct(int((df['form_factor'].fillna('').astype(str).str.strip() != '').sum()), total)}%",
            "use": "CTV vs mobile vs desktop/device class filters.",
            "caution": "Use watch-hour weighted marts before making business-share statements.",
        },
        {
            "field": "Brand / model",
            "status": "Ready for known values",
            "coverage": f"{pct(stats['identity_known'], total)}%",
            "use": "Device brand/model cards, CTV model drilldowns, unknown model backlog.",
            "caution": "Unknown and generic app UAs must remain explicit, not hidden.",
        },
        {
            "field": "OS / browser",
            "status": "Ready",
            "coverage": f"{pct(int((df['os_display'] != 'Unknown / NA').sum()), total)}%",
            "use": "OS distribution, browser/player troubleshooting, compatibility view.",
            "caution": "Some app/player UAs do not expose full browser data.",
        },
        {
            "field": "App / player",
            "status": "Ready as UA signal",
            "coverage": f"{pct(int((df['app_name'] != 'Unknown / NA').sum()), total)}%",
            "use": "Identify Tivimate, VLC, ExoPlayer, IPTV tooling, SDK/player families.",
            "caution": "App name is not the same as physical device model.",
        },
        {
            "field": "Usage-weighted device share",
            "status": "Pipeline WIP",
            "coverage": "Needs join to watch-hours/lake rows",
            "use": "Audience Ops charts by actual watch hours, IPs, sessions, devices.",
            "caution": "This preview is distinct-UA weighted only.",
        },
    ]
    api_samples = df[df["api_has_device_detail"]].sort_values(["api_brand", "api_model", "ua_norm"]).head(24)
    unknown_samples = df[df["decode_status"].eq("unknown")].copy()
    unknown_samples["_score"] = unknown_samples["ua_norm"].fillna("").astype(str).str.len()
    unknown_samples = unknown_samples.sort_values("_score", ascending=False).head(24)
    malformed_samples = df[df["decode_status"].eq("malformed")].copy()
    malformed_samples["_score"] = malformed_samples["ua_norm"].fillna("").astype(str).str.len()
    malformed_samples = malformed_samples.sort_values("_score", ascending=False).head(24)

    model_code_rows = []
    coded = df[df["model_code"].fillna("").astype(str).str.strip() != ""].copy()
    if not coded.empty:
        grouped = (
            coded.groupby(["model_code", "brand", "model", "product_family", "generation"], dropna=False)
            .agg(count=("ua_hash", "count"), ua_norm=("ua_norm", "first"))
            .reset_index()
            .sort_values("count", ascending=False)
            .head(30)
        )
        model_code_rows = sample_rows(
            grouped,
            ["model_code", "brand", "model", "product_family", "generation", "count", "ua_norm"],
            30,
        )

    explorer_cols = [
        "decode_status", "form_factor", "device_type", "brand", "model", "os_display",
        "browser_display", "app_name", "api_brand", "api_model", "ua_norm",
    ]
    explorer = df.sort_values(["decode_status", "form_factor", "brand", "model", "ua_norm"])
    payload = {
        "generated_at": datetime.now().strftime("%d/%m/%y %I:%M:%S %p IST"),
        "api_progress": {"done": done, "total": api_total, "latest_line": latest_line},
        "stats": stats,
        "insight_cards": insight_cards,
        "dashboard_readiness": dashboard_readiness,
        "form_factor": top_records(df, "form_factor", 14),
        "decode_status": top_records(df, "decode_status", 12),
        "device_type": top_records(df, "device_type", 14),
        "brands": top_records(df, "brand_display", 16),
        "models": top_records(df[df["model_display"] != "Unknown / NA"], "model_display", 16),
        "os_detail": top_records(df, "os_display", 16),
        "browser_detail": top_records(df, "browser_display", 16),
        "app_player": top_records(df, "app_name", 16),
        "ctv_brands": top_records(ctv, "brand_display", 14),
        "ctv_models": top_records(ctv[ctv["model_display"] != "Unknown / NA"], "model_display", 14),
        "ctv_os": top_records(ctv, "os_display", 14),
        "api_detail_quality": top_records(df[df["decode_status"].eq("decoded_api")], "api_detail_level", 12),
        "unknown_patterns": top_records(df[df["decode_status"].isin(["unknown", "malformed"])], "unknown_pattern", 14),
        "brand_os_matrix": table_records_from_counts(
            df[df["brand_display"] != "Unknown / NA"],
            ["brand_display", "os_display"],
            24,
        ),
        "identity_families": table_records_from_counts(
            df[df["identity_known"]],
            ["form_factor", "brand_display", "model_display"],
            24,
        ),
        "model_codes": model_code_rows,
        "api_samples": sample_rows(
            api_samples,
            ["api_device_type", "api_brand", "api_model", "api_os_name", "api_browser_name", "ua_norm"],
            24,
        ),
        "unknown_samples": sample_rows(
            unknown_samples,
            ["form_factor", "os_display", "app_name", "notes", "ua_norm"],
            24,
        ),
        "malformed_samples": sample_rows(malformed_samples, ["notes", "ua_norm"], 24),
        "explorer": sample_rows(explorer, explorer_cols, 5000),
    }
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate deep UA decode preview dashboard.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--map", type=Path, default=DEFAULT_MAP)
    parser.add_argument("--api-cache", type=Path, default=DEFAULT_API_CACHE)
    parser.add_argument("--log", type=Path, default=DEFAULT_LOG)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()

    decoded, api_cache = build_decoded(args.input, args.map, args.api_cache)
    payload = build_payload(decoded, api_cache, latest_api_progress(args.log))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(html_doc(payload), encoding="utf-8")
    print(args.out)
    print(json.dumps(payload["stats"], indent=2))


if __name__ == "__main__":
    main()
