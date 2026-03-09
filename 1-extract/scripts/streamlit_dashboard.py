"""
Data Sandbox Dashboard
----------------------
A high-performance Streamlit dashboard for real-time monitoring of 
sandbox resource utilization. Features live telemetry, historical 
data visualization, and system diagnostics.
"""

import streamlit as st
import psutil
import docker
import os
import time
import sqlite3
import pandas as pd
import sys
import threading
import plotly.graph_objects as go
from data_generator import GenerateData

# --- DATABASE CONFIGURATION ---
# Default paths (will be overridden when called from unified dashboard)
DB_DIR = "data"
DB_PATH = os.path.join(DB_DIR, "monitoring.db")

def set_db_path(path):
    global DB_PATH
    DB_PATH = path
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

def init_db():
    """Initializes the tracking database and handles schema migrations."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS metrics
                 (timestamp DATETIME, 
                  cpu_pct REAL, 
                  mem_mb REAL,
                  mem_pct REAL)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS extraction_logs
                 (timestamp DATETIME,
                  end_time DATETIME,
                  method TEXT,
                  records INTEGER,
                  processes INTEGER,
                  status TEXT,
                  workers INTEGER,
                  batches INTEGER)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS sensor_data
                 (timestamp DATETIME,
                  ph_level REAL,
                  ec_tds REAL,
                  water_temp REAL,
                  air_temp REAL,
                  humidity INTEGER,
                  water_level INTEGER)''')
    
    # Resilient schema check/updates
    try:
        c.execute("PRAGMA table_info(extraction_logs)")
        cols = [col[1] for col in c.fetchall()]
        if 'workers' not in cols:
            c.execute("ALTER TABLE extraction_logs ADD COLUMN workers INTEGER")
        if 'batches' not in cols:
            c.execute("ALTER TABLE extraction_logs ADD COLUMN batches INTEGER")
    except sqlite3.OperationalError:
        pass

    conn.commit()
    conn.close()

def log_metrics(cpu, mem, pct, ts=None):
    """Logs resource telemetry snapshot."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    if ts:
        c.execute("INSERT INTO metrics (timestamp, cpu_pct, mem_mb, mem_pct) VALUES (?, ?, ?, ?)", (ts, cpu, mem, pct))
    else:
        c.execute("INSERT INTO metrics (timestamp, cpu_pct, mem_mb, mem_pct) VALUES (datetime('now', 'localtime'), ?, ?, ?)", (cpu, mem, pct))
    conn.commit()
    conn.close()

def get_history(hours):
    """Retrieves historical metrics for observation window."""
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(
            f"SELECT * FROM metrics WHERE timestamp >= datetime('now', 'localtime', '-{hours} hours') ORDER BY timestamp ASC", 
            conn, parse_dates=['timestamp']
        )
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df

def get_latest_data(limit=50):
    """Fetches sensor records for preview."""
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(f"SELECT * FROM sensor_data ORDER BY timestamp DESC LIMIT {limit}", conn)
    except Exception:
        df = pd.DataFrame(columns=['timestamp', 'ph_level', 'ec_tds', 'water_temp', 'air_temp', 'humidity', 'water_level'])
    conn.close()
    return df

def get_extraction_logs(limit=10):
    """Retrieves history of extraction attempts."""
    conn = sqlite3.connect(DB_PATH)
    try:
        query = f"""
            SELECT rowid as sno, timestamp as start_time, end_time, method, records as count, 
                   processes, workers, batches, status,
                   (julianday(end_time) - julianday(timestamp)) * 86400 as duration_sec
            FROM extraction_logs ORDER BY start_time DESC LIMIT {limit}
        """
        df = pd.read_sql_query(query, conn)
    except Exception:
        df = pd.DataFrame(columns=['sno', 'start_time', 'end_time', 'method', 'count', 'processes', 'workers', 'batches', 'status', 'duration_sec'])
    conn.close()
    return df

def log_extraction_run(method, records, processes, workers, batches, status):
    """Records an extraction attempt."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT INTO extraction_logs (timestamp, method, records, processes, workers, batches, status) VALUES (datetime('now', 'localtime'), ?, ?, ?, ?, ?, ?)",
        (method, records, processes, workers, batches, status)
    )
    rowid = c.lastrowid
    conn.commit()
    conn.close()
    return rowid

def update_extraction_status(rowid, status):
    """Updates status/end_time for a run."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("UPDATE extraction_logs SET status = ?, end_time = datetime('now', 'localtime') WHERE rowid = ?", (status, rowid))
    conn.commit()
    conn.close()

def run_generator_wrapper(method, records, processes, gen_params, rowid):
    """Thread wrapper for data generation."""
    try:
        generator = GenerateData(**gen_params)
        generator.start_generating()
        update_extraction_status(rowid, "SUCCESS")
    except Exception as e:
        update_extraction_status(rowid, f"FAILED: {str(e).split('\\n')[0][:50]}")

def render_dashboard(stage_name="Extract", api_port=8000):
    """Main dashboard rendering entry point."""
    global DB_PATH
    init_db()

    # --- THEME & STYLING ---
    st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@200;300;400;500;600&display=swap');
        [data-testid="stStatusWidget"] { visibility: hidden !important; }
        #stDecoration { display: none !important; }
        .stApp > header { background: transparent !important; }
        .stApp { background-color: #0d0d0f; color: #e2e2e7; font-family: 'Outfit', sans-serif !important; }
        .resource-card-small {
            background: rgba(255, 255, 255, 0.03); backdrop-filter: blur(12px);
            border: 1px solid rgba(173, 212, 229, 0.1); border-radius: 12px;
            padding: 12px 15px; margin-bottom: 10px; display: flex; flex-direction: column;
            justify-content: space-between; min-height: 130px; transition: transform 0.3s ease;
        }
        .resource-card-small:hover { transform: translateY(-3px); border: 1px solid #017CC3; }
        .card-label { font-size: 0.6rem; color: #017CC3; text-transform: uppercase; font-weight: 500; letter-spacing: 1px; }
        .card-value-small { font-size: 1.4rem; font-weight: 600; color: #white; margin-bottom: 0.25rem; }
        .progress-bar { background: rgba(1, 124, 195, 0.1); height: 6px; border-radius: 3px; width: 100%; overflow: hidden; }
        .progress-fill { height: 100%; border-radius: 3px; background: linear-gradient(90deg, #ADD4E5, #017CC3); }
        .section-title-wrap { display: flex; align-items: center; gap: 15px; margin-bottom: 1rem; }
        .section-title-text { font-size: 1.5rem; font-weight: 600; color: #017CC3; margin: 0; }
        .section-title-line { flex-grow: 1; height: 1px; background: linear-gradient(90deg, #ADD4E5, transparent); }
    </style>
    """, unsafe_allow_html=True)

    st.markdown(f'''
    <div class="section-title-wrap">
        <div style="width: 5px; height: 30px; background: #017CC3; border-radius: 3px;"></div>
        <h2 class="section-title-text">{stage_name} Pipeline Diagnostics</h2>
        <div class="section-title-line"></div>
    </div>
    ''', unsafe_allow_html=True)

    col_tele, col_params = st.columns([2, 1], gap="large")

    with col_params:
        with st.container(border=True):
            st.markdown('<p style="font-size: 0.7rem; color: #ADD4E5; font-weight: 600;">CONFIG</p>', unsafe_allow_html=True)
            sc1, sc2 = st.columns(2)
            num_records = sc1.number_input("Records", 1, 1000000, 100, key=f"recs_{stage_name}")
            num_processes = sc1.slider("Processes", 1, 8, 1, key=f"proc_{stage_name}")
            method = sc2.selectbox("Method", ["stream", "batch"], key=f"meth_{stage_name}")
            if method == "stream":
                thread_workers = sc2.slider("Threads", 1, 20, 5, key=f"thr_{stage_name}")
            else:
                num_batches = sc2.number_input("Batches", 1, 100, 5, key=f"bat_{stage_name}")
            
            if st.button(f"Start {stage_name}", use_container_width=True, type="primary", key=f"btn_{stage_name}"):
                gen_params = {"num_records": num_records, "num_process": num_processes, "method": method, "port": api_port}
                if method == "stream": gen_params["thread_workers"] = thread_workers
                else: gen_params["num_batches"] = num_batches
                
                rid = log_extraction_run(method, num_records, num_processes, 
                                       thread_workers if method == "stream" else None,
                                       num_batches if method == "batch" else None, "RUNNING")
                threading.Thread(target=run_generator_wrapper, args=(method, num_records, num_processes, gen_params, rid), daemon=True).start()
                st.toast(f"Initiated {stage_name} pipeline...")

    def get_metrics():
        try:
            client = docker.from_env()
            container = client.containers.get("sandbox")
            stats = next(container.stats(stream=False))
            
            cpu_delta = stats["cpu_stats"]["cpu_usage"]["total_usage"] - stats["precpu_stats"].get("cpu_usage", {}).get("total_usage", 0)
            sys_delta = stats["cpu_stats"].get("system_cpu_usage", 0) - stats["precpu_stats"].get("system_cpu_usage", 0)
            online_cpus = stats["cpu_stats"].get("online_cpus", 1)
            cpu_pct = (cpu_delta / sys_delta) * online_cpus * 100.0 if sys_delta > 0 else 0.0
            
            mem_usage = stats["memory_stats"].get("usage", 0)
            mem_mb = mem_usage / (1024 * 1024)
            mem_pct = (mem_usage / stats["memory_stats"].get("limit", 1)) * 100.0
            
            return {"status": "Active", "cpu": cpu_pct, "mem_mb": mem_mb, "mem_pct": mem_pct, "ts": stats.get("read")}
        except:
            return {"status": "Inactive", "cpu": 0.0, "mem_mb": 0.0, "mem_pct": 0.0, "ts": None}

    with col_tele:
        time_filter = st.selectbox("Window", ["1H", "4H", "12H"], index=0, key=f"win_{stage_name}")
        
        @st.fragment(run_every=2)
        def display_metrics():
            m = get_metrics()
            log_metrics(m['cpu'], m['mem_mb'], m['mem_pct'], m['ts'])
            
            c1, c2 = st.columns([3, 1])
            with c1:
                df = get_history(int(time_filter[:-1]))
                if not df.empty:
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['cpu_pct'], name='CPU', fill='tozeroy', line=dict(color='#017CC3')))
                    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['mem_mb'], name='RAM', fill='tozeroy', line=dict(color='#ADD4E5'), yaxis='y2'))
                    fig.update_layout(template="plotly_dark", height=250, margin=dict(l=0, r=0, t=0, b=0), showlegend=False,
                                    yaxis2=dict(overlaying='y', side='right'))
                    st.plotly_chart(fig, use_container_width=True, key=f"chart_{stage_name}")
            with c2:
                st.markdown(f'''
                <div class="resource-card-small">
                    <div class="card-label">CPU</div>
                    <div class="card-value-small">{m['cpu']:.1f}%</div>
                    <div class="progress-bar"><div class="progress-fill" style="width: {min(m['cpu'], 100)}%"></div></div>
                </div>
                <div class="resource-card-small">
                    <div class="card-label">RAM</div>
                    <div class="card-value-small">{m['mem_mb']:.0f}MB</div>
                    <div class="progress-bar"><div class="progress-fill" style="width: {min(m['mem_pct'], 100)}%"></div></div>
                </div>
                ''', unsafe_allow_html=True)
        display_metrics()

    st.markdown('<div style="height: 1rem;"></div>', unsafe_allow_html=True)
    
    @st.fragment(run_every=2)
    def display_data():
        bl, br = st.columns(2)
        with bl:
            st.markdown(f'<p style="font-size: 0.8rem; color: #ADD4E5; font-weight: 600;">{stage_name.upper()} DATA</p>', unsafe_allow_html=True)
            st.dataframe(get_latest_data(10), hide_index=True, use_container_width=True, height=250)
        with br:
            st.markdown(f'<p style="font-size: 0.8rem; color: #ADD4E5; font-weight: 600;">SYSTEM LOGS</p>', unsafe_allow_html=True)
            log_df = get_extraction_logs(10)
            if not log_df.empty:
                log_df['duration'] = log_df['duration_sec'].apply(lambda x: f"{x:.4f}s" if pd.notnull(x) else "---")
                st.dataframe(log_df[['start_time', 'method', 'count', 'status', 'duration']], hide_index=True, use_container_width=True, height=250)
    display_data()

if __name__ == "__main__":
    st.set_page_config(page_title="Data Sandbox", page_icon="✨", layout="wide")
    render_dashboard()