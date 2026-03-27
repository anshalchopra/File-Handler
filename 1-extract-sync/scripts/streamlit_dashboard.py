"""
Midnight Pro: Synchronous Baseline Dashboard
Zero-Flicker Architecture with Integrated Pipeline Telemetry & Audit Logs.
"""

import streamlit as st
import psutil
import os
import sqlite3
import threading
import time
import subprocess
import sys
import pandas as pd
import plotly.graph_objects as go
import logging
import uuid
from datetime import datetime, timedelta

# --- 1. LOGGING CONFIG ---
LOG_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "logs", "pipeline.log"))
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("SyncDashboard")

st.set_page_config(page_title="Execute Pipeline Sync", page_icon="💡", layout="wide")

# ── CONFIG & DB ───────────────────────────────────────────────────────────────
DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "monitoring.db"))
DT_FMT = '%Y-%m-%d %H:%M:%S'

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    c = conn.cursor()
    # Enable WAL mode
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("""CREATE TABLE IF NOT EXISTS metrics
                 (timestamp DATETIME, cpu_pct REAL, mem_mb REAL, mem_pct REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS execution_logs
                 (timestamp DATETIME, end_time DATETIME, method TEXT,
                  records INTEGER, processes INTEGER, batches INTEGER, status TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS sensor_data
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp DATETIME, ph_level REAL, ec_tds REAL,
                  water_temp REAL, air_temp REAL, humidity INTEGER, water_level INTEGER)""")
    # Cleanup stale runs
    c.execute(
        "UPDATE execution_logs SET status='STALE', end_time=? WHERE status='RUNNING'",
        (datetime.now().strftime(DT_FMT),)
    )
    conn.commit()
    conn.close()

def now_str() -> str:
    return datetime.now().strftime(DT_FMT)

def log_metrics(cpu, mem_mb, mem_pct):
    try:
        conn = sqlite3.connect(DB_PATH, timeout=60.0)
        conn.execute(
            "INSERT INTO metrics (timestamp, cpu_pct, mem_mb, mem_pct) VALUES (?,?,?,?)",
            (now_str(), cpu, mem_mb, mem_pct)
        )
        conn.commit()
        conn.close()
    except: pass

def get_metrics_latest(hours=1):
    conn = sqlite3.connect(DB_PATH)
    cutoff = (datetime.now() - timedelta(hours=hours)).strftime(DT_FMT)
    df = pd.read_sql_query("SELECT * FROM metrics WHERE timestamp >= ? ORDER BY timestamp ASC", conn, params=(cutoff,))
    conn.close()
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp'], format=DT_FMT, errors='coerce')
    return df

def get_sensor_data(limit=50):
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    try:
        df = pd.read_sql_query("SELECT * FROM sensor_data ORDER BY id DESC LIMIT ?", conn, params=(limit,))
    except: df = pd.DataFrame()
    finally: conn.close()
    return df

def get_execution_logs():
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    try:
        df = pd.read_sql_query("""
            SELECT
                timestamp   AS start_time,
                method,
                records     AS count,
                status,
                CASE
                    WHEN end_time IS NOT NULL AND end_time != ''
                    THEN ROUND((julianday(end_time) - julianday(timestamp)) * 86400, 1)
                    ELSE NULL
                END AS duration_s
            FROM execution_logs
            ORDER BY timestamp DESC LIMIT 20""", conn)
    except: df = pd.DataFrame()
    finally: conn.close()
    return df

def log_run(method, records, status='RUNNING'):
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    c = conn.cursor()
    c.execute(
        "INSERT INTO execution_logs (timestamp, method, records, processes, batches, status) "
        "VALUES (?,?,?,1,1,?)",
        (now_str(), method, records, status)
    )
    rid = c.lastrowid
    conn.commit()
    conn.close()
    return rid

def finish_run(rowid, status):
    import time
    # RETRY LOOP: Don't give up on the SUCCESS badge just because the disk is busy!
    for _ in range(5):
        try:
            conn = sqlite3.connect(DB_PATH, timeout=60.0)
            conn.execute("PRAGMA busy_timeout = 60000")
            conn.execute(
                "UPDATE execution_logs SET status=?, end_time=? WHERE rowid=?",
                (status, now_str(), rowid)
            )
            conn.commit()
            conn.close()
            return
        except:
            time.sleep(2)

def get_active_runs():
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    c = conn.cursor()
    c.execute("PRAGMA busy_timeout = 30000")
    c.execute("SELECT count(*) FROM execution_logs WHERE status='RUNNING'")
    count = c.fetchone()[0]
    conn.close()
    return count

# ── HARDWARE ENGINE ──
def get_system_stats():
    LIMIT_RAM = 2048.0
    LIMIT_CPU = 4.0
    try:
        cpu_p = psutil.cpu_percent(interval=None)
        mem_mb = psutil.Process().memory_info().rss / (1024**2)
        return {
            "cpu_pct": cpu_p,
            "cpu_used": (cpu_p/100)*LIMIT_CPU,
            "mem_pct": min((mem_mb/LIMIT_RAM)*100, 100.0),
            "mem_used": mem_mb,
        }
    except:
        return {"cpu_pct": 0, "cpu_used": 0, "mem_pct": 0, "mem_used": 0}

# ── WORKER ──
def run_generator(params, rid):
    try:
        # PURE SEQUENTIAL: Silence output to prevent memory jams in the dashboard thread
        cmd = [sys.executable, os.path.join(os.path.dirname(__file__), "data_generator.py"),
               "--records", str(params["num_records"]),
               "--method", params["method"],
               "--port", str(params.get("port", 9000)),
               "--batches", str(params.get("num_batches", 1))]
        
        # DEVNULL: Don't track 1,000,000 progress lines in RAM!
        result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        if result.returncode == 0:
            finish_run(rid, "SUCCESS")
        else:
            finish_run(rid, "FAILED")
            
    except Exception:
        finish_run(rid, "FAILED")

# ── CSS ──
st.markdown("""
<style>
    [data-testid="stStatusWidget"], #stDecoration { visibility: hidden; }
    .stApp { background: #0A0C14 !important; color: #E2E8F0; }
    .status-pill { padding: 5px 14px; border-radius: 20px; font-size: 0.62rem; font-weight: 700; display: inline-flex; align-items: center; gap: 7px; text-transform: uppercase; }
    .status-pill.active { background: rgba(0,229,255,0.08); color: #00E5FF; border: 1px solid rgba(0,229,255,0.4); }
    .status-pill.idle   { background: rgba(100,116,139,0.1); color: #64748B; border: 1px solid rgba(100,116,139,0.25); }
    .pulse { width: 6px; height: 6px; border-radius: 50%; background: currentColor; animation: p-anim 1.4s infinite; }
    @keyframes p-anim { 0%,100%{opacity:1} 50%{opacity:0.3} }
    .sec-label { font-size: 0.62rem; color: #FBBF24; font-weight: 700; letter-spacing: 2px; text-transform: uppercase; margin: 16px 0 8px 0; display: flex; align-items: center; gap: 8px; }
    .sec-label::after { content: ''; flex: 1; height: 1px; background: rgba(251,191,36,0.1); }
</style>
""", unsafe_allow_html=True)

# ── MAIN ──
def main():
    init_db()
    
    header_p = st.empty()
    
    st.markdown('<div class="sec-label" style="color:#FBBF24">Sequential Control</div>', unsafe_allow_html=True)
    with st.container(border=True):
        c1, c2, c3 = st.columns([1.5, 1, 1], gap="medium")
        with c1: n_recs = st.number_input("Count", 1, 1000000, 500)
        with c2: 
            method = st.selectbox("Mode", ["stream", "batch"])
            batches = st.number_input("Batches", 1, 100, 1) if method == "batch" else 1
        with c3:
            st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True) # Spacer
            if st.button("🚀  Start Sync Wave", use_container_width=True, type="primary"):
                rid = log_run(method, n_recs)
                params = {"num_records": n_recs, "method": method, "num_batches": batches, "port": 9000}
                t = threading.Thread(target=run_generator, args=(params, rid), daemon=True)
                t.start()
                st.toast("Sync Pipeline Launched!")

    st.markdown('<div class="sec-label">Baseline Telemetry</div>', unsafe_allow_html=True)
    col_hud, col_chart = st.columns([0.8, 2.5], gap="large")
    with col_hud: hud_p = st.empty()
    with col_chart: chart_p = st.empty()

    st.divider()
    col_t1, col_t2 = st.columns(2)
    with col_t1: data_p = st.empty()
    with col_t2: logs_p = st.empty()
    audit_p = st.empty()

    # THE PULSE
    current_run_id = str(uuid.uuid4())
    st.session_state.run_id = current_run_id

    while st.session_state.run_id == current_run_id:
        m = get_system_stats()
        log_metrics(m['cpu_pct'], m['mem_used'], m['mem_pct'])
        active = get_active_runs()
        
        status_cls = "active" if active > 0 else "idle"
        label = f"SEQUENTIAL SYNC · {active} ACTIVE" if active > 0 else "BASELINE READY"
        
        header_p.markdown(f"""
        <div style="display:flex;justify-content:space-between;align-items:center;padding-bottom:10px;">
            <h2 style="margin:0;font-size:1.4rem;">Midnight <span style="color:#64748B;font-weight:300;">Sync</span> Dashboard</h2>
            <div class="status-pill {status_cls}"><div class="pulse"></div>{label}</div>
        </div>
        """, unsafe_allow_html=True)

        hud_p.markdown(f"""
        <div style="display:flex;flex-direction:column;gap:12px;">
            <div style="background:rgba(251,191,36,0.06);border:1px solid rgba(251,191,36,0.15);border-radius:12px;padding:18px;text-align:center;">
                <div style="color:#64748B;font-size:0.6rem;text-transform:uppercase;letter-spacing:1px;margin-bottom:6px;">CPU Load (Sync)</div>
                <div style="color:#FBBF24;font-size:1.8rem;font-weight:800;">{m['cpu_pct']:.1f}%</div>
                <div style="color:#475569;font-size:0.65rem;">Sequentially Bound</div>
            </div>
            <div style="background:rgba(0,229,255,0.06);border:1px solid rgba(0,229,255,0.15);border-radius:12px;padding:18px;text-align:center;">
                <div style="color:#64748B;font-size:0.6rem;text-transform:uppercase;letter-spacing:1px;margin-bottom:6px;">Baseline Memory</div>
                <div style="color:#00E5FF;font-size:1.8rem;font-weight:800;">{m['mem_used']:.2f}<span style="font-size:1rem;margin-left:4px;">MB</span></div>
                <div style="color:#475569;font-size:0.65rem;">Flat Concurrency</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        df = get_metrics_latest()
        fig = go.Figure()
        if not df.empty:
            fig.add_trace(go.Scattergl(x=df['timestamp'], y=df['cpu_pct'], name='CPU', line=dict(color='#FBBF24', width=3), yaxis="y1"))
            fig.add_trace(go.Scattergl(x=df['timestamp'], y=df['mem_pct'], name='RAM', line=dict(color='#00E5FF', width=3), fill='tozeroy', fillcolor='rgba(0,229,255,0.05)', yaxis="y2"))
            x_min, x_max = df['timestamp'].min(), df['timestamp'].max() + timedelta(seconds=15)
        else: x_min, x_max = datetime.now() - timedelta(minutes=5), datetime.now() + timedelta(seconds=15)

        fig.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', margin=dict(l=40, r=40, t=10, b=30), height=300, uirevision='const',
                          yaxis=dict(title="CPU %", range=[0, 100]), yaxis2=dict(title="RAM %", range=[0, 100], overlaying='y', side='right'), xaxis=dict(range=[x_min, x_max]))

        chart_p.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
        with data_p.container():
            st.markdown('<div class="sec-label" style="color:#00E5FF">Latest Sync Records</div>', unsafe_allow_html=True)
            st.dataframe(get_sensor_data(), hide_index=True, use_container_width=True, height=250)
        with logs_p.container():
            st.markdown('<div class="sec-label" style="color:#FBBF24">Sync Run History</div>', unsafe_allow_html=True)
            st.dataframe(get_execution_logs(), hide_index=True, use_container_width=True, height=250)

        time.sleep(1)

if __name__ == "__main__":
    main()