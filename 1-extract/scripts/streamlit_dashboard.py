"""
Midnight Pro: High-Speed Execution Dashboard
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
from datetime import datetime, timedelta

# --- 1. LOGGING CONFIG ---
LOG_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "logs", "pipeline.log"))
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("Dashboard")

st.set_page_config(page_title="Execute Pipeline Pro", page_icon="🚀", layout="wide")

# ── CONFIG & DB ───────────────────────────────────────────────────────────────
DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "monitoring.db"))
DT_FMT = '%Y-%m-%d %H:%M:%S'

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    c = conn.cursor()
    # Enable WAL mode for concurrent read/write
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
        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        conn.execute(
            "INSERT INTO metrics (timestamp, cpu_pct, mem_mb, mem_pct) VALUES (?,?,?,?)",
            (now_str(), cpu, mem_mb, mem_pct)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Metrics Log Error: {e}")

def get_metrics_latest(hours=1):
    conn = sqlite3.connect(DB_PATH)
    cutoff = (datetime.now() - timedelta(hours=hours)).strftime(DT_FMT)
    df = pd.read_sql_query(
        "SELECT * FROM metrics WHERE timestamp >= ? ORDER BY timestamp ASC",
        conn, params=(cutoff,)
    )
    conn.close()
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp'], format=DT_FMT, errors='coerce')
    return df

def get_sensor_data(limit=50):
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    try:
        df = pd.read_sql_query(
            "SELECT id, timestamp, ph_level, ec_tds, water_temp, air_temp, humidity, water_level "
            "FROM sensor_data ORDER BY id DESC LIMIT ?",
            conn, params=(limit,)
        )
    except:
        df = pd.DataFrame()
    finally:
        conn.close()
    return df

def get_execution_logs():
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    try:
        df = pd.read_sql_query("""
            SELECT
                timestamp   AS start_time,
                method,
                records     AS count,
                processes   AS workers,
                batches,
                status,
                CASE
                    WHEN end_time IS NOT NULL AND end_time != ''
                    THEN ROUND((julianday(end_time) - julianday(timestamp)) * 86400, 1)
                    ELSE NULL
                END AS duration_s
            FROM execution_logs
            ORDER BY timestamp DESC LIMIT 20""",
            conn
        )
    except:
        df = pd.DataFrame()
    finally:
        conn.close()
    return df

def log_run(method, records, processes, batches):
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    c = conn.cursor()
    c.execute(
        "INSERT INTO execution_logs (timestamp, method, records, processes, batches, status) "
        "VALUES (?,?,?,?,?,'RUNNING')",
        (now_str(), method, records, processes, batches)
    )
    rid = c.lastrowid
    conn.commit()
    conn.close()
    return rid

def finish_run(rowid, status):
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    conn.execute(
        "UPDATE execution_logs SET status=?, end_time=? WHERE rowid=?",
        (status, now_str(), rowid)
    )
    conn.commit()
    conn.close()

def get_active_runs():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT count(*) FROM execution_logs WHERE status='RUNNING'")
    count = c.fetchone()[0]
    conn.close()
    return count

# ── HARDWARE ENGINE (PIPELINE NATIVE + PULSE) ─────────────────────────────────
def get_system_stats():
    """Reports Dashboard + Worker tree usage vs 512MB / 1.0 Core limit with EMA smoothing."""
    # Ensure persistent process tracking in session state
    if 'proc_tree' not in st.session_state:
        try:
            main_p = psutil.Process()
            st.session_state.proc_tree = [main_p] + main_p.children(recursive=True)
            # Pre-initialize CPU counters
            for p in st.session_state.proc_tree: p.cpu_percent(None)
        except:
            st.session_state.proc_tree = []
            
    if 'ema_cpu' not in st.session_state: st.session_state.ema_cpu = 0.0
    if 'ema_mem' not in st.session_state: st.session_state.ema_mem = 0.0

    try:
        LIMIT_RAM = 2048.0
        LIMIT_CPU = 4.0
        
        # Periodic tree refresh (every ~10s or if empty)
        if not st.session_state.proc_tree or time.time() % 10 < 0.2:
            main_p = psutil.Process()
            st.session_state.proc_tree = [main_p] + main_p.children(recursive=True)

        mem_mb = 0
        cpu_sum = 0
        for p in st.session_state.proc_tree:
            try:
                if p.is_running():
                    mem_mb += p.memory_info().rss / (1024**2)
                    cpu_sum += p.cpu_percent(None)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        
        # Apply heavier smoothing (EMA) for a fluid, professional feel
        alpha = 0.2 # Lower = smoother, less jumpy
        st.session_state.ema_cpu = alpha * cpu_sum + (1 - alpha) * st.session_state.ema_cpu
        st.session_state.ema_mem = alpha * mem_mb + (1 - alpha) * st.session_state.ema_mem
        
        cpu_p = min(st.session_state.ema_cpu, 100.0)
        mem_mb_val = st.session_state.ema_mem

        return {
            "cpu_pct": cpu_p,
            "cpu_used": (cpu_p/100)*LIMIT_CPU,
            "cpu_total": LIMIT_CPU,
            "mem_pct": min((mem_mb_val/LIMIT_RAM)*100, 100.0),
            "mem_used": mem_mb_val,
            "mem_total": LIMIT_RAM
        }
    except:
        return {"cpu_pct": 0, "cpu_used": 0, "cpu_total": 1, "mem_pct": 0, "mem_used": 0, "mem_total": 512}


# ── WORKER ────────────────────────────────────────────────────────────────────

def run_generator(params, rid):
    try:
        cmd = [sys.executable, os.path.join(os.path.dirname(__file__), "data_generator.py"),
               "--records", str(params["num_records"]),
               "--processes", str(params["num_process"]),
               "--method", params["method"],
               "--port", str(params.get("port", 8000)),
               "--batches", str(params.get("num_batches", 1))]
        subprocess.run(cmd, check=True)
        finish_run(rid, "SUCCESS")
    except Exception as e:
        logger.error(f"Execution Error: {e}")
        finish_run(rid, "FAILED")

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    [data-testid="stStatusWidget"], #stDecoration { visibility: hidden; }
    .stApp { background: #0A0C14 !important; color: #E2E8F0; }
    .h-metrics-container { display: flex; flex-direction: column; gap: 10px; margin-top: 14px; }
    .h-metric-box { background: rgba(255,255,255,0.025); border: 1px solid rgba(255,255,255,0.07); border-radius: 10px; padding: 12px 16px; }
    .h-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; }
    .h-title { color: #64748B; font-size: 0.68rem; font-weight: 600; text-transform: uppercase; letter-spacing: 1.2px; }
    .h-val   { color: #F1F5F9; font-size: 0.78rem; font-weight: 500; }
    .h-bar-bg { width: 100%; height: 4px; background: rgba(255,255,255,0.06); border-radius: 2px; overflow: hidden; }
    .h-bar-fill { height: 100%; border-radius: 2px; transition: width 0.4s ease; }
    .status-pill { padding: 5px 14px; border-radius: 20px; font-size: 0.62rem; font-weight: 700; display: inline-flex; align-items: center; gap: 7px; text-transform: uppercase; }
    .status-pill.active { background: rgba(0,229,255,0.08); color: #00E5FF; border: 1px solid rgba(0,229,255,0.4); }
    .status-pill.idle   { background: rgba(100,116,139,0.1); color: #64748B; border: 1px solid rgba(100,116,139,0.25); }
    .pulse { width: 6px; height: 6px; border-radius: 50%; background: currentColor; animation: p-anim 1.4s infinite; }
    @keyframes p-anim { 0%,100%{opacity:1} 50%{opacity:0.3} }
    .sec-label { font-size: 0.62rem; color: #00E5FF; font-weight: 700; letter-spacing: 2px; text-transform: uppercase; margin: 16px 0 8px 0; display: flex; align-items: center; gap: 8px; }
    .sec-label::after { content: ''; flex: 1; height: 1px; background: rgba(0,229,255,0.1); }
</style>
""", unsafe_allow_html=True)

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    init_db()
    
    # Placeholders
    header_p = st.empty()
    
    # ── 1. CONFIGURATION (TOP) ────────────────────────────────────────────────
    st.markdown('<div class="sec-label">Control Center</div>', unsafe_allow_html=True)
    with st.container(border=True):
        c1, c2, c3, c4 = st.columns([1.2, 1, 1, 1], gap="medium")
        with c1: n_recs = st.number_input("Count", 1, 1000000, 500)
        with c2: n_proc = st.slider("Workers", 1, 8, 1)
        with c3: 
            method = st.selectbox("Mode", ["stream", "batch"])
            batches = 1
            if method == "batch":
                batches = st.number_input("Batches", 1, 100, 1)
        with c4:
            st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True) # Spacer
            if st.button("🚀  Execute Wave", use_container_width=True, type="primary"):
                rid = log_run(method, n_recs, n_proc, batches)
                params = {"num_records": n_recs, "num_process": n_proc, "method": method, "num_batches": batches}
                t = threading.Thread(target=run_generator, args=(params, rid), daemon=True)
                t.start()
                st.toast("Pipeline Launched!")

    st.markdown('<div class="sec-label">Real-Time Telemetry</div>', unsafe_allow_html=True)
    col_hud, col_chart = st.columns([0.8, 2.5], gap="large")
    
    with col_hud:
        hud_p = st.empty()
    with col_chart:
        chart_p = st.empty()

    st.divider()
    col_t1, col_t2 = st.columns(2)
    with col_t1: data_p = st.empty()
    with col_t2: logs_p = st.empty()
    st.divider()
    audit_p = st.empty()

    # --- 2. THE PULSE (LOOP GUARD) ---
    # We use a run_id to ensure only ONE loop exists per window.
    # If the user clicks a button, Streamlit reruns the script, and this new CID 
    # will cause the OLD loop below to 'break' immediately.
    import uuid
    current_run_id = str(uuid.uuid4())
    st.session_state.run_id = current_run_id

    # ── High-Speed Telemetry Loop ──
    while st.session_state.run_id == current_run_id:
        m = get_system_stats()
        log_metrics(m['cpu_pct'], m['mem_used'], m['mem_pct'])
        active = get_active_runs()
        
        status_cls = "active" if active > 0 else "idle"
        label = f"INGESTING · {active} ACTIVE" if active > 0 else "SYSTEM READY"
        
        # 1. Update Header
        header_p.markdown(f"""
        <div style="display:flex;justify-content:space-between;align-items:center;padding-bottom:10px;">
            <h2 style="margin:0;font-size:1.4rem;">Midnight <span style="color:#64748B;font-weight:300;">Pro</span> Dashboard</h2>
            <div class="status-pill {status_cls}"><div class="pulse"></div>{label}</div>
        </div>
        """, unsafe_allow_html=True)

        # 2. Update HUD
        hud_p.markdown(f"""
        <div style="display:flex;flex-direction:column;gap:12px;">
            <div style="background:rgba(167,139,250,0.06);border:1px solid rgba(167,139,250,0.15);border-radius:12px;padding:18px;text-align:center;">
                <div style="color:#64748B;font-size:0.6rem;text-transform:uppercase;letter-spacing:1px;margin-bottom:6px;">Current CPU Load</div>
                <div style="color:#A78BFA;font-size:1.8rem;font-weight:800;">{m['cpu_pct']:.1f}%</div>
                <div style="color:#475569;font-size:0.65rem;">{m['cpu_used']:.2f} Cores Active</div>
            </div>
            <div style="background:rgba(0,229,255,0.06);border:1px solid rgba(0,229,255,0.15);border-radius:12px;padding:18px;text-align:center;">
                <div style="color:#64748B;font-size:0.6rem;text-transform:uppercase;letter-spacing:1px;margin-bottom:6px;">Sandbox Memory</div>
                <div style="color:#00E5FF;font-size:1.8rem;font-weight:800;">{m['mem_used']:.2f}<span style="font-size:1rem;margin-left:4px;">MB</span></div>
                <div style="color:#475569;font-size:0.65rem;">{(m['mem_pct'] if m['mem_pct'] <= 100 else 100):.1f}% Unit Capacity</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        # 3. Update Charts & Tables
        df = get_metrics_latest()
        fig = go.Figure()
        if not df.empty:
            fig.add_trace(go.Scattergl(x=df['timestamp'], y=df['cpu_pct'], name='CPU Load', line=dict(color='#A78BFA', width=3), yaxis="y1"))
            fig.add_trace(go.Scattergl(x=df['timestamp'], y=df['mem_pct'], name='RAM Usage', line=dict(color='#00E5FF', width=3), fill='tozeroy', fillcolor='rgba(0,229,255,0.05)', yaxis="y2"))
            
            # Add 10s breathing room on the right side of the timeline
            x_min = df['timestamp'].min()
            x_max = df['timestamp'].max() + timedelta(seconds=15) # Increased to 15s to be safe
        else:
            x_min, x_max = datetime.now() - timedelta(minutes=5), datetime.now() + timedelta(seconds=15)

        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            margin=dict(l=40, r=40, t=10, b=30),
            height=300,
            uirevision='const',
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            yaxis=dict(title="CPU %", range=[0, 100], gridcolor='rgba(255,255,255,0.05)', zeroline=False),
            yaxis2=dict(title="RAM %", range=[0, 100], overlaying='y', side='right', gridcolor='rgba(255,255,255,0.05)', zeroline=False),
            xaxis=dict(range=[x_min, x_max], gridcolor='rgba(255,255,255,0.05)', zeroline=False)
        )

        chart_p.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
        
        with data_p.container():
            st.markdown('<div class="sec-label">Latest Records</div>', unsafe_allow_html=True)
            st.dataframe(get_sensor_data(), hide_index=True, use_container_width=True, height=250)
        
        with logs_p.container():
            st.markdown('<div class="sec-label">Run History</div>', unsafe_allow_html=True)
            st.dataframe(get_execution_logs(), hide_index=True, use_container_width=True, height=250)

        with audit_p.container():
            st.markdown('<div class="sec-label">Audit Trail</div>', unsafe_allow_html=True)
            try:
                with open(LOG_PATH, "r") as f:
                    st.code("".join(f.readlines()[-10:]), language="log")
            except: pass

        # Smooth heartbeat refresh
        time.sleep(1)

if __name__ == "__main__":
    main()