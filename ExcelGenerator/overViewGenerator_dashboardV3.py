"""
generate_dashboard.py  —  Enterprise Edition (Averages Restored)
──────────────────────────────────────────────────────────────────────────────────
Key Updates:
- RESTORED: Dynamic dashed average lines on all charts.
- MAINTAINED: Fix for bracket syntax error on all percentage indices.
- MAINTAINED: Bound-safe column fetcher prevents IndexError on truncated Excel rows.
- MAINTAINED: Python-side date parsing normalizes regional text dates.
- MAINTAINED: Date range filter (7D/30D/ALL) with full chart & table syncing.

Usage:  python generate_dashboard.py
"""

import sys, json, urllib.request
from pathlib import Path
from datetime import datetime

try:
    import openpyxl
except ImportError:
    import subprocess; subprocess.check_call([sys.executable,"-m","pip","install","openpyxl","--quiet"])
    import openpyxl

# ╔══════════════════════════════════════════════════════════╗
# ║  SET THESE PATHS                                         ║
# ╚══════════════════════════════════════════════════════════╝
XLSX_PATH = Path(r"Y:\Veto Logs Backup\Dashboards\OverView\overview_report.xlsx")
HTML_OUT  = Path(r"Y:\Veto Logs Backup\Dashboards\OverView\overview_dashboard.html")
# ══════════════════════════════════════════════════════════

if len(sys.argv) == 3:
    XLSX_PATH, HTML_OUT = Path(sys.argv[1]), Path(sys.argv[2])

COL = {
    'DATE': 2, 'BYTES': 3, 'ROWS': 4, 'IP_ROWS': 5,
    'DIST_IP': 6, 'DIST_IPUA': 7, 'DIST_DEV': 8, 'DIST_SESS': 9,
    'DIST_IP_R2': 10, 'DIST_IPUA_R2': 11, 
    'SESS_AVAIL': 12, 'SESS_NA': 13, 'SESS_NONE': 14,
    'PCT_IP': 15, 'PCT_IPUA': 16, 'PCT_SESS': 17, 'PCT_SESSNA': 18, 'PCT_NONE': 19
}
DATA_START_ROW = 13

def sn(v):
    if v is None: return 0
    try: return float(v)
    except: return 0

def get_val(row, col_num):
    """Safely fetch cell value by 1-based index to prevent unexpected IndexError on short rows"""
    idx = col_num - 1
    return row[idx] if idx < len(row) else None

CHARTJS_CACHE = Path(__file__).parent / ".chartjs_cache.js"
CHARTJS_URL   = "https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"

def get_chartjs():
    if CHARTJS_CACHE.exists():
        return CHARTJS_CACHE.read_text(encoding="utf-8")
    print("  Downloading Chart.js once...", end=" ", flush=True)
    try:
        req = urllib.request.Request(CHARTJS_URL, headers={'User-Agent':'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=20) as r:
            js = r.read().decode("utf-8")
        CHARTJS_CACHE.write_text(js, encoding="utf-8")
        print(f"done ({len(js)//1024}KB cached)")
        return js
    except Exception as e:
        print(f"failed: {e}\n  Charts will be disabled.")
        return None

print(f"Reading: {XLSX_PATH}")
if not XLSX_PATH.exists():
    print(f"  ERROR: File not found: {XLSX_PATH}"); sys.exit(1)

wb = openpyxl.load_workbook(XLSX_PATH, read_only=True, data_only=True)
ws = wb.active

meta = {}
exclude_keywords = ["report generated", "filtered to", "lake folder", "duck db", "parquet files", "date range"]
for row in ws.iter_rows(min_row=2, max_row=9, values_only=True):
    k = str(row[1]).strip() if row[1] else ''
    v = str(row[2]).strip() if row[2] else ''
    if k and k not in ('None','nan') and not any(keyword in k.lower() for keyword in exclude_keywords):
        meta[k] = v

data_rows = []
for row in ws.iter_rows(min_row=DATA_START_ROW, values_only=True):
    raw_d = get_val(row, COL['DATE'])
    if isinstance(raw_d, datetime):
        ds = raw_d.strftime("%Y-%m-%d")
    elif raw_d:
        ds = str(raw_d).strip()
        if not ds or 'TOTAL' in ds.upper() or ds in ('None','nan'): continue
        
        # Normalize regional variations (e.g., 14/05/26 to standard 2026-05-14) on Python side
        parsed_dt = None
        for fmt_str in ("%d/%m/%y", "%d/%m/%Y", "%Y-%m-%d", "%d-%m-%y", "%d-%m-%Y"):
            try:
                parsed_dt = datetime.strptime(ds, fmt_str)
                break
            except ValueError:
                continue
        if parsed_dt:
            ds = parsed_dt.strftime("%Y-%m-%d")
        else:
            if '/' not in ds and '-' not in ds: continue
    else:
        continue
        
    if not ds or 'TOTAL' in ds.upper(): continue

    ip  = int(sn(get_val(row, COL['IP_ROWS'])))
    sa  = int(sn(get_val(row, COL['SESS_AVAIL'])))
    sna = int(sn(get_val(row, COL['SESS_NA'])))
    sno = int(sn(get_val(row, COL['SESS_NONE'])))
    
    dist_ip = int(sn(get_val(row, COL['DIST_IP'])))
    dist_ipua = int(sn(get_val(row, COL['DIST_IPUA'])))
    
    dip_r2 = get_val(row, COL['DIST_IP_R2'])
    dipua_r2 = get_val(row, COL['DIST_IPUA_R2'])
    dist_ip_r2 = int(sn(dip_r2)) if dip_r2 not in (None, '', 'None', 'nan') else dist_ip
    dist_ipua_r2 = int(sn(dipua_r2)) if dipua_r2 not in (None, '', 'None', 'nan') else dist_ipua

    p_ip = get_val(row, COL['PCT_IP'])
    p_ipua = get_val(row, COL['PCT_IPUA'])
    p_sess = get_val(row, COL['PCT_SESS'])
    p_sessna = get_val(row, COL['PCT_SESSNA'])
    p_none = get_val(row, COL['PCT_NONE'])
    
    data_rows.append({
        "date":          ds,
        "bytes":         round(sn(get_val(row, COL['BYTES'])), 2),
        "rows":          int(sn(get_val(row, COL['ROWS']))),
        "ip_rows":       ip,
        "dist_ip":       dist_ip,
        "dist_ipua":     dist_ipua,
        "dist_dev":      int(sn(get_val(row, COL['DIST_DEV']))),
        "dist_sess":     int(sn(get_val(row, COL['DIST_SESS']))),
        "dist_ip_r2":    dist_ip_r2,
        "dist_ipua_r2":  dist_ipua_r2,
        "sess_avail":    sa,
        "sess_na":       sna,
        "sess_none":     sno,
        "pct_ip":        sn(p_ip) if (p_ip not in (None, '', 'None', 'nan')) else (round(dist_ip / ip, 6) if ip > 0 else 0),
        "pct_ipua":      sn(p_ipua) if (p_ipua not in (None, '', 'None', 'nan')) else (round(dist_ipua / ip, 6) if ip > 0 else 0),
        "pct_sess":      sn(p_sess) if (p_sess not in (None, '', 'None', 'nan')) else (round(sa / ip, 6) if ip > 0 else 0),
        "pct_sessna":    sn(p_sessna) if (p_sessna not in (None, '', 'None', 'nan')) else (round(sna / ip, 6) if ip > 0 else 0),
        "pct_none":      sn(p_none) if (p_none not in (None, '', 'None', 'nan')) else (round(sno / ip, 6) if ip > 0 else 0),
    })
wb.close()

data_blob = json.dumps({
    "meta": meta, "rows": data_rows,
    "generated": datetime.now().strftime("%Y-%m-%d %H:%M"),
    "source": XLSX_PATH.name,
}, ensure_ascii=False, separators=(',', ':'))

chartjs = get_chartjs()
chartjs_tag = f"<script>{chartjs}</script>" if chartjs else "<script>window.Chart=null;</script>"

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Overview Dashboard</title>
""" + chartjs_tag + """
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f0f2f5;color:#1a1a2e;min-height:100vh}
.topbar{background:#1F3864;color:#fff;padding:14px 28px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px}
.topbar h1{font-size:16px;font-weight:600}.topbar .sub{font-size:11.5px;opacity:.6}
#main{padding:20px 26px;max-width:1700px;margin:0 auto}
.meta-strip{background:#fff;border-radius:10px;border:1px solid #e2e8f0;padding:12px 18px;margin-bottom:16px;display:flex;flex-wrap:wrap;gap:8px 26px;font-size:12px;color:#555}
.meta-strip b{color:#1a1a2e;font-weight:600}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin-bottom:20px}
.card{background:#fff;border-radius:10px;border:1px solid #e2e8f0;padding:15px 17px;transition:all 0.3s}
.clbl{font-size:11px;color:#888;margin-bottom:5px;display:flex;align-items:center;gap:5px}
.cdot{width:8px;height:8px;border-radius:50%;flex-shrink:0}
.cval{font-size:22px;font-weight:700;line-height:1.1}.csub{font-size:10px;color:#bbb;margin-top:4px}
.charts{display:grid;grid-template-columns:repeat(auto-fit, minmax(350px, 1fr));gap:14px;margin-bottom:20px}
.cbox{background:#fff;border-radius:10px;border:1px solid #e2e8f0;padding:14px 17px}
.cbox h3{font-size:12px;font-weight:600;margin-bottom:10px;display:flex;justify-content:space-between}
.table-wrap{background:#fff;border-radius:10px;border:1px solid #e2e8f0;overflow:hidden;margin-bottom:18px}
.thead{padding:12px 17px;display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid #e2e8f0;flex-wrap:wrap;gap:15px}
.thead h3{font-size:13px;font-weight:600}
.filter-group{display:flex;align-items:center;gap:8px;background:#f8fafc;padding:5px 8px;border-radius:8px;border:1px solid #e2e8f0}
.filter-group input[type="date"]{border:1px solid #cbd5e1;padding:4px 6px;border-radius:4px;font-size:11px;outline:none;font-family:inherit;color:#1F3864;font-weight:500;cursor:pointer}
.filter-group span{font-size:11px;color:#888;font-weight:500}
.btn-preset{background:#fff;border:1px solid #cbd5e1;padding:4px 10px;border-radius:4px;font-size:11px;font-weight:600;cursor:pointer;color:#555;transition:all 0.2s}
.btn-preset:hover{background:#edf2f7;color:#1F3864}
.btn-preset.active{background:#1F3864;color:#fff;border-color:#1F3864}
.tbl-c{overflow-x:auto;max-height:650px;overflow-y:auto}
table{width:100%;border-collapse:collapse;font-size:11px}
thead{position:sticky;top:0;z-index:5}
thead tr:first-child th{padding:6px 10px;font-size:10px;font-weight:700;color:#fff;text-align:center;white-space:nowrap}
thead tr:last-child th{padding:8px 10px;text-align:right;font-size:10px;font-weight:600;color:#444;border-bottom:1px solid #e2e8f0;white-space:nowrap;background:#f8fafc;cursor:pointer;user-select:none}
thead tr:last-child th:hover{background:#edf2f7}
thead tr:last-child th:first-child{text-align:left}
td{padding:7px 10px;text-align:right;border-bottom:.5px solid #f0f0f0;white-space:nowrap}
td:first-child{text-align:left;font-weight:500;position:sticky;left:0;background:inherit;z-index:1;min-width:85px}
tr:nth-child(even) td{background:#fafafa}
tr:hover td{background:#eef4ff!important}
tr.total-row td{background:#1F3864!important;color:#fff!important;font-weight:700}
.pct{color:#185FA5;font-weight:500}
.footer{text-align:center;font-size:11px;color:#ccc;padding:8px 0 20px}
th.b{background:#1F3864} th.g{background:#2e6b10} th.m{background:#7c3aed} th.a{background:#9a6000} th.p{background:#4a41a8}
</style>
</head>
<body>
<div class="topbar">
  <h1>&#128202;&nbsp; Complete 18-Column Enterprise Dashboard</h1>
  <span class="sub" id="genLbl"></span>
</div>
<div id="main">
  <div class="meta-strip" id="metaStrip"></div>
  <div class="cards" id="cards"></div>
  
  <div class="charts">
    <div class="cbox"><h3><span>Daily Row Volume</span><span id="cH1" style="color:#aaa;font-weight:400"></span></h3><canvas id="cRows" height="170"></canvas></div>
    <div class="cbox"><h3><span>Unique Visitors & Devices</span><span id="cH2" style="color:#aaa;font-weight:400"></span></h3><canvas id="cIP" height="170"></canvas></div>
    <div class="cbox"><h3><span>Session Health Status</span><span id="cH3" style="color:#aaa;font-weight:400"></span></h3><canvas id="cSess" height="170"></canvas></div>
    <div class="cbox"><h3><span>Data Density Profile (GiB)</span><span id="cH4" style="color:#aaa;font-weight:400"></span></h3><canvas id="cBytes" height="170"></canvas></div>
  </div>

  <div class="table-wrap">
    <div class="thead">
      <h3>Day-by-Day Breakdown (Full Schema)</h3>
      <div class="filter-group">
         <button class="btn-preset active" id="btnAll" onclick="setPreset('all')">ALL</button>
         <button class="btn-preset" id="btn30" onclick="setPreset(30)">30D</button>
         <button class="btn-preset" id="btn7" onclick="setPreset(7)">7D</button>
         <span style="margin:0 4px">|</span>
         <input type="date" id="dateStart" onchange="handleCustomDate()" />
         <span>to</span>
         <input type="date" id="dateEnd" onchange="handleCustomDate()" />
      </div>
    </div>
    <div class="tbl-c"><table id="tbl"></table></div>
  </div>
  <div class="footer" id="footer"></div>
</div>
<script>
const {meta,rows,generated,source} = """ + data_blob + """;

// Parse strictly using localized component metrics to remove browser engine discrepancies
rows.forEach(r => { 
    const [y, m, d] = r.date.split('-').map(Number);
    r.ts = new Date(y, m - 1, d).getTime(); 
});
rows.sort((a,b) => a.ts - b.ts);

document.getElementById('genLbl').textContent='Last updated '+generated+' | Source: '+source;
document.getElementById('footer').textContent='Refresh by running generate_dashboard.py';
if(Object.keys(meta).length > 0) document.getElementById('metaStrip').innerHTML=Object.entries(meta).map(([k,v])=>`<span><b>${k}:</b> ${v}</span>`).join('');
else document.getElementById('metaStrip').style.display = 'none';

const fmt=(n,d=0)=>n==null||isNaN(n)?'\u2014':Number(n).toLocaleString('en-IN',{minimumFractionDigits:d,maximumFractionDigits:d});
const fmtP=n=>{ if(n==null||isNaN(n))return'\u2014'; const p=Math.abs(n)<=1?n*100:n; return p.toFixed(2)+'%'; };

let viewRows = [...rows];
let sortKey = null;
let sortAsc = true;
const chartInstances = {};

const CD={tension:.35,fill:true,pointRadius:1.5,pointHoverRadius:4,borderWidth:1.5,animation:{duration:400}};
const mkOpt=yfmt=>({responsive:true,plugins:{legend:{position:'bottom',labels:{font:{size:9.5},boxWidth:8,padding:6}}},
  scales:{x:{ticks:{font:{size:8.5},maxTicksLimit:12},grid:{color:'#f5f5f5'}},
          y:{ticks:{font:{size:8.5},callback:v=>yfmt==='gib'?v.toFixed(0)+' G':v>=1e6?(v/1e6).toFixed(1)+'M':v>=1e3?(v/1e3).toFixed(0)+'K':v},grid:{color:'#f5f5f5'}}}});

function initCharts() {
  if(typeof Chart==='undefined') return;
  const render = (id, datasets, yfmt) => { chartInstances[id] = new Chart(document.getElementById(id), { type:'line', data:{labels:[], datasets:datasets}, options:mkOpt(yfmt) }); };
  
  // Notice the x2 pairing structure. Dataset 0 is actual data, Dataset 1 is the dashed average line.
  render('cRows', [
      {label:'Total rows', borderColor:'#185FA5', backgroundColor:'rgba(24,95,165,.04)', ...CD},
      {label:'Avg', borderColor:'rgba(24,95,165,.6)', borderWidth:1.5, borderDash:[5,5], pointRadius:0, fill:false}
  ]);
  
  render('cIP', [
      {label:'Distinct cliIP', borderColor:'#4a41a8', backgroundColor:'transparent', ...CD},
      {label:'Avg', borderColor:'rgba(74,65,168,.5)', borderWidth:1.5, borderDash:[5,5], pointRadius:0, fill:false},
      {label:'Distinct device', borderColor:'#0F6E56', backgroundColor:'transparent', ...CD},
      {label:'Avg', borderColor:'rgba(15,110,86,.5)', borderWidth:1.5, borderDash:[5,5], pointRadius:0, fill:false}
  ]);
  
  render('cSess', [
      {label:'Sess Avail', borderColor:'#2e6b10', backgroundColor:'transparent', ...CD},
      {label:'Avg', borderColor:'rgba(46,107,16,.5)', borderWidth:1.5, borderDash:[5,5], pointRadius:0, fill:false},
      {label:'Sess Missing', borderColor:'#9a6000', backgroundColor:'transparent', ...CD},
      {label:'Avg', borderColor:'rgba(154,96,0,.5)', borderWidth:1.5, borderDash:[5,5], pointRadius:0, fill:false},
      {label:'Sess Not Found', borderColor:'#b71c1c', backgroundColor:'transparent', ...CD},
      {label:'Avg', borderColor:'rgba(183,28,28,.5)', borderWidth:1.5, borderDash:[5,5], pointRadius:0, fill:false}
  ]);
  
  render('cBytes', [
      {label:'GiB Data', borderColor:'#1D9E75', backgroundColor:'rgba(29,158,117,.04)', ...CD},
      {label:'Avg', borderColor:'rgba(29,158,117,.6)', borderWidth:1.5, borderDash:[5,5], pointRadius:0, fill:false}
  ], 'gib');
}

function updateKPIs() {
    const ndays = viewRows.length || 1;
    const tBytes = viewRows.reduce((s, r) => s + r.bytes, 0);
    const tIP = viewRows.reduce((s, r) => s + r.dist_ip, 0);
    const tSess = viewRows.reduce((s, r) => s + r.dist_sess, 0);
    const tRows = viewRows.reduce((s, r) => s + r.rows, 0);

    const kpiHTML = [
        {l:'Volume in View',v:fmt(tBytes,2)+' G',clr:'#0F6E56',s:'cumulative transfer for range'},
        {l:'Distinct IPs in View',v:fmt(tIP),clr:'#4a41a8',s:'unique absolute users for range'},
        {l:'Total Sessions in View',v:fmt(tSess),clr:'#9a6000',s:'tracked unique sessions for range'},
        {l:'Avg Rows / Day',v:fmt(Math.round(tRows/ndays)),clr:'#b71c1c',s:'mean daily logs for range'}
    ].map(c => `<div class="card"><div class="clbl"><span class="cdot" style="background:${c.clr}"></span>${c.l}</div><div class="cval" style="color:${c.clr}">${c.v}</div><div class="csub">${c.s}</div></div>`).join('');
    document.getElementById('cards').innerHTML = kpiHTML;
}

function updateCharts() {
  if(!chartInstances['cRows']) return;
  const lbl = viewRows.map(r=>r.date);
  const len = viewRows.length || 1;
  const vTot = k => viewRows.reduce((s,r) => s+r[k], 0);

  const sync = (id, dataMaps) => {
    const ch = chartInstances[id]; 
    ch.data.labels = lbl;
    dataMaps.forEach((k, i) => { 
        ch.data.datasets[i * 2].data = viewRows.map(r => r[k]); // The actual data
        ch.data.datasets[(i * 2) + 1].data = Array(len).fill(vTot(k)/len); // The flat average line
    });
    ch.update();
  };
  
  sync('cRows', ['rows']); 
  sync('cIP', ['dist_ip', 'dist_dev']);
  sync('cSess', ['sess_avail', 'sess_na', 'sess_none']); 
  sync('cBytes', ['bytes']);
  document.querySelectorAll('.cbox h3 span+span').forEach(e => e.textContent = viewRows.length + ' days visible');
}

const subH=['Date (IST)', 'Total Bytes (GiB)', 'Total Rows*', 'cliIP rows', 'Distinct cliIP', 'Distinct cliIP & UA', 'Distinct device_id', 'Distinct session_id', 'Distinct cliIP', 'Distinct cliIP & UA', 'Session Found - ID Available', 'Session Found - ID NOT Available', 'Session Not Found', 'Distinct cliIP', 'Distinct cliIP & UA', 'Session Found - ID Available', 'Session Found - ID NOT Available', 'Session Not Found'];
const fields=['date','bytes','rows','ip_rows','dist_ip','dist_ipua','dist_dev','dist_sess','dist_ip_r2','dist_ipua_r2','sess_avail','sess_na','sess_none','pct_ip','pct_ipua','pct_sess','pct_sessna','pct_none'];

function renderTable() {
  let h = `<thead>
    <tr><th class="b" style="background:#f8fafc;"></th><th colspan="3" class="b">General Volumes</th><th colspan="4" class="g">Absolute Counts</th><th colspan="2" class="m">Unique Subsets</th><th colspan="3" class="a">Session Identifiers</th><th colspan="5" class="p">% Data of TOTAL Segment</th></tr>
    <tr>${subH.map((title, idx) => `<th onclick="handleSort('${fields[idx]}')">${title}${sortKey === fields[idx] ? (sortAsc ? ' \u25B2' : ' \u25BC') : ''}</th>`).join('')}</tr>
  </thead><tbody>`;

  h += viewRows.map(d => `<tr>
    <td>${d.date}</td><td>${fmt(d.bytes,2)}</td><td>${fmt(d.rows)}</td><td>${fmt(d.ip_rows)}</td>
    <td>${fmt(d.dist_ip)}</td><td>${fmt(d.dist_ipua)}</td><td>${fmt(d.dist_dev)}</td><td>${fmt(d.dist_sess)}</td>
    <td>${fmt(d.dist_ip_r2)}</td><td>${fmt(d.dist_ipua_r2)}</td>
    <td>${fmt(d.sess_avail)}</td><td>${fmt(d.sess_na)}</td><td>${fmt(d.sess_none)}</td>
    <td class="pct">${fmtP(d.pct_ip)}</td><td class="pct">${fmtP(d.pct_ipua)}</td><td class="pct">${fmtP(d.pct_sess)}</td><td class="pct">${fmtP(d.pct_sessna)}</td><td class="pct">${fmtP(d.pct_none)}</td>
  </tr>`).join('');

  const tKeys = ["bytes","rows","ip_rows","dist_ip","dist_ipua","dist_dev","dist_sess","dist_ip_r2","dist_ipua_r2","sess_avail","sess_na","sess_none"];
  const tVals = {}; tKeys.forEach(k => tVals[k] = viewRows.reduce((sum, r) => sum + r[k], 0));

  h += `<tr class="total-row">
    <td>TOTAL</td><td>${fmt(tVals.bytes,2)}</td><td>${fmt(tVals.rows)}</td><td>${fmt(tVals.ip_rows)}</td>
    <td>${fmt(tVals.dist_ip)}</td><td>${fmt(tVals.dist_ipua)}</td><td>${fmt(tVals.dist_dev)}</td><td>${fmt(tVals.dist_sess)}</td>
    <td>${fmt(tVals.dist_ip_r2)}</td><td>${fmt(tVals.dist_ipua_r2)}</td>
    <td>${fmt(tVals.sess_avail)}</td><td>${fmt(tVals.sess_na)}</td><td>${fmt(tVals.sess_none)}</td>
    <td colspan="5" style="text-align:center;opacity:.7;font-size:10px">— dynamic visible snapshot totals —</td>
  </tr></tbody>`;
  document.getElementById('tbl').innerHTML = h;
}

function handleSort(key) {
  if (sortKey === key) sortAsc = !sortAsc;
  else { sortKey = key; sortAsc = true; }
  executeSortLogic(); renderTable();
}

function executeSortLogic() {
  viewRows.sort((a, b) => {
    let va = a[sortKey], vb = b[sortKey];
    if(sortKey === 'date') return sortAsc ? a.ts - b.ts : b.ts - a.ts;
    if (typeof va === 'string') return sortAsc ? va.localeCompare(vb) : vb.localeCompare(va);
    return sortAsc ? va - vb : vb - va;
  });
}

function syncUIState(presetMode) {
    document.querySelectorAll('.btn-preset').forEach(b => b.classList.remove('active'));
    if(presetMode === 'all') document.getElementById('btnAll').classList.add('active');
    else if(presetMode === 30) document.getElementById('btn30').classList.add('active');
    else if(presetMode === 7) document.getElementById('btn7').classList.add('active');
}

function setPreset(days) {
    syncUIState(days);
    if(days === 'all' || rows.length === 0) {
        viewRows = [...rows];
        document.getElementById('dateStart').value = '';
        document.getElementById('dateEnd').value = '';
    } else {
        const maxTs = rows[rows.length - 1].ts;
        const targetTs = maxTs - (days * 86400000);
        viewRows = rows.filter(r => r.ts >= targetTs);
        
        const dtFmt = (ts) => { 
            const d = new Date(ts); 
            return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`; 
        };
        document.getElementById('dateStart').value = dtFmt(viewRows[0].ts);
        document.getElementById('dateEnd').value = dtFmt(viewRows[viewRows.length-1].ts);
    }
    if(sortKey) executeSortLogic();
    renderTable(); updateCharts(); updateKPIs();
}

function handleCustomDate() {
    syncUIState(null);
    const startStr = document.getElementById('dateStart').value;
    const endStr = document.getElementById('dateEnd').value;
    
    let sTs = -Infinity;
    if (startStr) {
        const [y, m, d] = startStr.split('-').map(Number);
        sTs = new Date(y, m - 1, d).getTime();
    }
    let eTs = Infinity;
    if (endStr) {
        const [y, m, d] = endStr.split('-').map(Number);
        eTs = new Date(y, m - 1, d + 1).getTime() - 1; 
    }
    
    viewRows = rows.filter(r => r.ts >= sTs && r.ts <= eTs);
    if(sortKey) executeSortLogic();
    renderTable(); updateCharts(); updateKPIs();
}

initCharts();
updateKPIs();
updateCharts();
renderTable();
</script>
</body>
</html>"""

HTML_OUT.parent.mkdir(parents=True, exist_ok=True)
HTML_OUT.write_text(HTML, encoding='utf-8')
print(f"\n[Success] Dashboard output to: {HTML_OUT}")