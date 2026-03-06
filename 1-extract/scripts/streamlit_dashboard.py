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
# Metrics are persisted in a local SQLite database for historical analysis
DB_DIR = "data"
os.makedirs(DB_DIR, exist_ok=True)
DB_PATH = os.path.join(DB_DIR, "monitoring.db")

def init_db():
    """
    Initializes the tracking database and handles schema migrations.
    """
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
                  status TEXT)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS sensor_data
                 (timestamp DATETIME,
                  ph_level REAL,
                  ec_tds REAL,
                  water_temp REAL,
                  air_temp REAL,
                  humidity INTEGER,
                  water_level INTEGER)''')
    
    # Perform resilient migrations for schema updates
    try:
        c.execute("ALTER TABLE metrics ADD COLUMN mem_pct REAL")
    except sqlite3.OperationalError:
        pass # Column already exists
        
    try:
        c.execute("ALTER TABLE extraction_logs ADD COLUMN end_time DATETIME")
    except sqlite3.OperationalError:
        pass # Column already exists
    
    conn.commit()
    conn.close()

def log_metrics(cpu, mem, pct, ts=None):
    """
    Logs a single snapshot of resource telemetry.
    
    Args:
        cpu (float): CPU Load percentage.
        mem (float): Memory usage in MB.
        pct (float): Memory usage percentage.
        ts (str, optional): Custom timestamp. Defaults to local server time.
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    if ts:
        c.execute("INSERT INTO metrics (timestamp, cpu_pct, mem_mb, mem_pct) VALUES (?, ?, ?, ?)", (ts, cpu, mem, pct))
    else:
        c.execute("INSERT INTO metrics (timestamp, cpu_pct, mem_mb, mem_pct) VALUES (datetime('now', 'localtime'), ?, ?, ?)", (cpu, mem, pct))
    conn.commit()
    conn.close()

def get_history(hours):
    """
    Retrieves historical metrics for a specific observational window.
    """
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
    """
    Fetches the most recent sensor records for the preview pane.
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(
            f"SELECT * FROM sensor_data ORDER BY timestamp DESC LIMIT {limit}", 
            conn
        )
    except Exception:
        df = pd.DataFrame(columns=['timestamp', 'ph_level', 'ec_tds', 'water_temp', 'air_temp', 'humidity', 'water_level'])
    conn.close()
    return df

def get_extraction_logs(limit=10):
    """
    Retrieves history of extraction attempts with microsecond duration.
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        query = f"""
            SELECT 
                rowid as sno,
                timestamp as start_time,
                end_time,
                method,
                records as count,
                status,
                (julianday(end_time) - julianday(timestamp)) * 86400 as duration_sec
            FROM extraction_logs 
            ORDER BY start_time DESC 
            LIMIT {limit}
        """
        df = pd.read_sql_query(query, conn)
    except Exception as e:
        print(f"Log fetch err: {e}")
        df = pd.DataFrame(columns=['sno', 'start_time', 'end_time', 'method', 'count', 'status', 'duration_sec'])
    conn.close()
    return df

def log_extraction_run(method, records, processes, status):
    """
    Records an extraction attempt in the history table.
    Returns the rowid for later updates.
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT INTO extraction_logs (timestamp, method, records, processes, status) VALUES (datetime('now', 'localtime'), ?, ?, ?, ?)",
        (method, records, processes, status)
    )
    rowid = c.lastrowid
    conn.commit()
    conn.close()
    return rowid

def update_extraction_status(rowid, status):
    """
    Updates the status and end_time of a specific extraction run.
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("UPDATE extraction_logs SET status = ?, end_time = datetime('now', 'localtime') WHERE rowid = ?", (status, rowid))
    conn.commit()
    conn.close()

def run_generator_wrapper(method, records, processes, gen_params, rowid):
    """
    Wrapper for background thread to track and log extraction completion.
    """
    print(f"🧵 Background thread started for {method} (rowid: {rowid})")
    try:
        generator = GenerateData(**gen_params)
        generator.start_generating()
        update_extraction_status(rowid, "SUCCESS")
        print(f"✅ Background thread finished successfully (rowid: {rowid})")
    except Exception as e:
        err = str(e)
        print(f"❌ Background thread FAILED (rowid: {rowid}): {err}")
        # Avoid huge error strings in the status column
        short_err = err.split('\n')[0][:50]
        update_extraction_status(rowid, f"FAILED: {short_err}")

# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="Data Sandbox",
    page_icon="✨",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# --- THEME & STYLING ENGINE ---
# High-end glassmorphic design system using CSS injection
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@200;300;400;500;600&display=swap');

    /* --- LAYOUT OVERRIDES --- */
    [data-testid="stStatusWidget"] { visibility: hidden !important; }
    #stDecoration { display: none !important; }
    .stApp > header { background: transparent !important; }
    [data-testid="stSidebar"] { display: none !important; }

    html, body, [class*="css"], .stMarkdown, p, span, div {
        font-family: 'Outfit', sans-serif !important;
        font-weight: 300;
    }

    .stApp {
        background-color: #0d0d0f;
        background-image: 
            radial-gradient(circle at 20% 20%, rgba(59, 130, 246, 0.05) 0%, transparent 40%),
            radial-gradient(circle at 80% 80%, rgba(139, 92, 246, 0.05) 0%, transparent 40%);
        color: #e2e2e7;
    }

    /* --- HERO COMPONENT --- */
    .hero-title {
        text-align: center;
        background: linear-gradient(135deg, #60a5fa 0%, #a78bfa 50%, #f472b6 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 3.5rem;
        font-weight: 600;
        margin-top: 1rem;
        margin-bottom: 0.25rem;
        letter-spacing: -2px;
        animation: fadeIn 1.5s ease-out;
    }

    .hero-subtitle {
        text-align: center;
        color: #94a3b8;
        font-size: 0.9rem;
        letter-spacing: 2px;
        text-transform: uppercase;
        margin-bottom: 1rem;
        opacity: 0.8;
    }

    /* --- TELEMETRY CARDS --- */
    .resource-card-small {
        background: rgba(255, 255, 255, 0.03);
        backdrop-filter: blur(12px);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 12px;
        padding: 12px 15px;
        text-align: left;
        margin-bottom: 10px;
        box-shadow: 0 4px 16px 0 rgba(0, 0, 0, 0.2);
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        min-height: 130px;
        transition: transform 0.3s ease;
        overflow: hidden;
    }

    .resource-card-small:hover {
        transform: translateY(-5px);
        border: 1px solid rgba(59, 130, 246, 0.3);
    }

    .card-label {
        font-size: 0.6rem;
        color: #94a3b8;
        text-transform: uppercase;
        font-weight: 500;
        letter-spacing: 1px;
        margin-bottom: 0.25rem;
    }

    .card-value-small {
        font-size: 1.4rem;
        font-weight: 600;
        color: #ffffff;
        margin-bottom: 0.35rem;
        letter-spacing: -1px;
        text-shadow: 0 0 20px rgba(255,255,255,0.1);
    }

    .card-footer-small {
        font-size: 0.65rem; 
        color: #64748b; 
        margin-top: 8px;
        display: flex;
        justify-content: space-between;
        flex-wrap: wrap;
        gap: 4px;
    }

    /* --- PROGRESS BARS --- */
    .progress-bar {
        background: rgba(255, 255, 255, 0.06);
        height: 6px;
        border-radius: 3px;
        width: 100%;
        overflow: hidden;
    }

    .progress-fill {
        height: 100%;
        border-radius: 3px;
        transition: width 0.8s cubic-bezier(0.1, 0.7, 1.0, 0.1);
        animation: pulse 2s infinite ease-in-out;
    }

    .fill-blue { background: linear-gradient(90deg, #3b82f6, #60a5fa); box-shadow: 0 0 15px rgba(59, 130, 246, 0.4); }
    .fill-purple { background: linear-gradient(90deg, #8b5cf6, #a78bfa); box-shadow: 0 0 15px rgba(139, 92, 246, 0.4); }


    /* --- SECTION HEADERS --- */
    .section-title-wrap {
        display: flex;
        align-items: center;
        gap: 15px;
        margin-bottom: 1rem;
        padding: 5px 0;
    }

    .section-title-line {
        flex-grow: 1;
        height: 1px;
        background: linear-gradient(90deg, rgba(59, 130, 246, 0.5), transparent);
    }

    .section-title-text {
        font-size: 1.8rem;
        font-weight: 600;
        letter-spacing: -0.5px;
        color: #f8fafc;
        margin: 0;
    }

    /* --- ANIMATIONS --- */
    @keyframes fadeIn {
        from { opacity: 0; transform: translateY(20px); }
        to { opacity: 1; transform: translateY(0); }
    }
    @keyframes pulse {
        0% { opacity: 0.6; }
        50% { opacity: 1; }
        100% { opacity: 0.6; }
    }

    div[data-baseweb="select"] {
        background-color: rgba(255, 255, 255, 0.03) !important;
        border-radius: 8px !important;
    }

    /* --- DATA TABLE STYLING --- */
    .data-container {
        background: rgba(255, 255, 255, 0.02);
        border: 1px solid rgba(255, 255, 255, 0.05);
        border-radius: 12px;
        padding: 10px;
        margin-top: 15px;
    }

    .stDataFrame {
        border-radius: 8px;
        overflow: hidden;
    }
    
    /* Control Box Styling */
    .control-box {
        background: rgba(255, 255, 255, 0.03);
        border: 1px solid rgba(255, 255, 255, 0.06);
        border-radius: 15px;
        padding: 20px;
    }
</style>
""", unsafe_allow_html=True)

# Initialize system data
init_db()

# --- EXTRACTION SIMULATOR SECTION ---
st.markdown('<div style="height: 1rem;"></div>', unsafe_allow_html=True)

st.markdown("""
<div class="section-title-wrap">
    <div style="width: 5px; height: 35px; background: linear-gradient(to bottom, #f472b6, #a78bfa); border-radius: 3px;"></div>
    <h2 class="section-title-text">Extraction Simulator</h2>
    <div class="section-title-line" style="background: linear-gradient(90deg, rgba(244, 114, 182, 0.5), transparent);"></div>
</div>
""", unsafe_allow_html=True)

# Main Container
col_sim_ctrl, col_sim_data = st.columns([1, 1], gap="large")

with col_sim_ctrl:
    st.markdown('<div class="control-box">', unsafe_allow_html=True)
    st.markdown('<p style="font-size: 0.8rem; color: #94a3b8; text-transform: uppercase; font-weight: 500; margin-bottom: 1.5rem;">Parameter Configuration</p>', unsafe_allow_html=True)
    
    sub_col1, sub_col2 = st.columns(2)
    with sub_col1:
        num_records = st.number_input("Total Records", min_value=1, max_value=1000000, value=100, step=100)
        num_processes = st.slider("Parallel Processes", min_value=1, max_value=8, value=1)
    
    with sub_col2:
        method = st.selectbox("Method", ["stream", "batch"], index=0)
        if method == "stream":
            thread_workers = st.slider("Thread Workers", min_value=1, max_value=20, value=5)
        else:
            num_batches = st.number_input("Batch Count", min_value=1, max_value=100, value=5)
    
    st.markdown('<div style="height: 1rem;"></div>', unsafe_allow_html=True)
    if st.button("🚀 Start Extraction", use_container_width=True, type="primary"):
        try:
            gen_params = {"num_records": num_records, "num_process": num_processes, "method": method, "port": 8000}
            if method == "stream": 
                gen_params["thread_workers"] = thread_workers
            else: 
                gen_params["num_batches"] = num_batches
                
            # Create the initial log record with RUNNING status
            rowid = log_extraction_run(method, num_records, num_processes, "RUNNING")
            
            # Run the generator in the background with the specific rowid to update
            thread = threading.Thread(
                target=run_generator_wrapper, 
                args=(method, num_records, num_processes, gen_params, rowid),
                daemon=True
            )
            thread.start()
            
            st.toast(f"🚀 {method.upper()} extraction initiated...")
        except Exception as e:
            st.error(f"Failed to start background process: {str(e)}")
    st.markdown('</div>', unsafe_allow_html=True)

# --- LIVE PREVIEW & LOGS FRAGMENT ---
@st.fragment(run_every=2)
def display_extraction_feedback():
    """
    Refreshes the Data Preview and Logs tables every 2 seconds automatically.
    """
    col_sim_data_inner, col_sim_empty = st.columns([1, 0.001]) # Inner layout for fragment
    
    with col_sim_data_inner:
        # Check for active runs to show loader
        latest_logs = get_extraction_logs(1)
        is_active = not latest_logs.empty and latest_logs.iloc[0]['status'] == 'RUNNING'

        header_col1, header_col2 = st.columns([2, 1])
        with header_col1:
            st.markdown('<p style="font-size: 0.8rem; color: #94a3b8; text-transform: uppercase; font-weight: 500; margin-bottom: 0.5rem; margin-top: 5px;">Data Ingestion Preview</p>', unsafe_allow_html=True)
        with header_col2:
            if is_active:
                st.markdown('<p style="font-size: 0.75rem; color: #f472b6; font-weight: 600; margin-top: 5px; animation: pulse 1s infinite;">● EXTRACTION ACTIVE</p>', unsafe_allow_html=True)

        # Live Data Preview Pane
        latest_df = get_latest_data(15)
        if not latest_df.empty:
            st.dataframe(
                latest_df,
                hide_index=True,
                column_config={
                    "timestamp": st.column_config.TextColumn("Captured At"),
                    "ph_level": st.column_config.NumberColumn("pH", format="%.2f"),
                    "ec_tds": st.column_config.NumberColumn("EC/TDS", format="%.2f"),
                    "water_temp": st.column_config.NumberColumn("W-Temp", format="%.1f°C"),
                    "air_temp": st.column_config.NumberColumn("A-Temp", format="%.1f°C"),
                    "humidity": st.column_config.NumberColumn("Humid", format="%d%%"),
                    "water_level": st.column_config.NumberColumn("Level", format="%d%%")
                },
                height=250, 
                use_container_width=True
            )
        else:
            st.info("No records in extraction pipe yet.")

        st.markdown('<div style="height: 1.5rem;"></div>', unsafe_allow_html=True)
        st.markdown('<p style="font-size: 0.8rem; color: #94a3b8; text-transform: uppercase; font-weight: 500; margin-bottom: 0.5rem;">System Extraction Logs</p>', unsafe_allow_html=True)
        
        # Extraction Log Preview
        log_df = get_extraction_logs(15)
        if not log_df.empty:
            # Format duration in seconds with 6 decimal places (microsecond precision)
            log_df['duration'] = log_df['duration_sec'].apply(lambda x: f"{x:.6f}s" if pd.notnull(x) else "---")
            
            # Filter only requested columns to hide end_time
            display_df = log_df[['sno', 'start_time', 'duration', 'method', 'count', 'status']]
            
            st.dataframe(
                display_df,
                hide_index=True,
                column_config={
                    "sno": st.column_config.NumberColumn("S.No"),
                    "start_time": st.column_config.TextColumn("Timestamp"),
                    "duration": st.column_config.TextColumn("Duration"),
                    "method": st.column_config.TextColumn("Method"),
                    "count": st.column_config.NumberColumn("Count"),
                    "status": st.column_config.TextColumn("Status")
                },
                height=250,
                use_container_width=True
            )
        else:
            st.info("System logs are clear.")

with col_sim_data:
    display_extraction_feedback()

# --- TELEMETRY SECTION ---
def get_metrics():
    """
    Fetches real-time container resource telemetry via Docker Stats API.
    
    Returns:
        dict: Processed metrics including CPU Load, Memory Usage, and Limits.
    """
    try:
        # Resolve Docker connection
        try:
            client = docker.from_env()
            client.ping()
        except Exception:
             client = docker.DockerClient(base_url="unix:///var/run/docker.sock")
                  
        container = client.containers.get("sandbox")
        # Fetch the latest snapshot from the stats stream
        stats_iterator = container.stats(stream=True, decode=True)
        stats = next(stats_iterator)
        
        # --- CPU TELEMETRY ---
        cpu_stats = stats["cpu_stats"]
        precpu_stats = stats["precpu_stats"]
        
        # Calculate Delta and Percentage
        cpu_delta = cpu_stats["cpu_usage"]["total_usage"] - precpu_stats.get("cpu_usage", {}).get("total_usage", 0)
        system_delta = cpu_stats.get("system_cpu_usage", 0) - precpu_stats.get("system_cpu_usage", 0)
        online_cpus = cpu_stats.get("online_cpus", 1)

        cpu_pct = (cpu_delta / system_delta) * online_cpus * 100.0 if system_delta > 0.0 else 0.0

        # --- MEMORY TELEMETRY ---
        mem_stats = stats["memory_stats"]
        mem_usage = mem_stats.get("usage", 0)
        mem_limit = mem_stats.get("limit", 1)
        
        mem_mb = mem_usage / (1024 * 1024)
        mem_limit_mb = mem_limit / (1024 * 1024)
        mem_pct = (mem_usage / mem_limit) * 100.0
        
        # Resolve Core limits
        cpu_limit_nano = container.attrs["HostConfig"]["NanoCpus"]
        max_cores = (cpu_limit_nano / 1e9) if cpu_limit_nano > 0 else online_cpus

        return {
            "status": "Running",
            "cpu_pct": round(cpu_pct, 1),
            "max_cores": round(max_cores, 2),
            "mem_val": f"{mem_mb:.1f} MB",
            "mem_mb": mem_mb,
            "mem_limit": f"{mem_limit_mb:.0f} MB",
            "mem_limit_num": mem_limit_mb,
            "mem_pct": mem_pct,
            "read_time": stats.get("read")
        }
    except Exception as e:
        # Fallback state for Stopped/Unreachable container
        return {
            "status": f"Stopped ({str(e).split(':')[0] if ':' in str(e) else str(e)})",
            "cpu_pct": 0.0,
            "max_cores": 0.0,
            "mem_val": "0.0 MB",
            "mem_mb": 0.0,
            "mem_limit": "0 MB",
            "mem_limit_num": 100,
            "mem_pct": 0.0
        }

# --- CONTROL PANEL ---
st.markdown('<div style="height: 1.5rem;"></div>', unsafe_allow_html=True)

st.markdown("""
<div class="section-title-wrap">
    <div style="width: 5px; height: 35px; background: linear-gradient(to bottom, #3b82f6, #8b5cf6); border-radius: 3px;"></div>
    <h2 class="section-title-text">Resource Telemetry</h2>
    <div class="section-title-line"></div>
</div>
""", unsafe_allow_html=True)

col_ctrl1, col_ctrl2 = st.columns([1, 4])
with col_ctrl1:
    time_filter = st.selectbox(
        "Observation Window", 
        ["1H", "4H", "8H", "12H"], 
        index=0,
        help="Historical timeframe for the telemetry chart."
    )
    hours_map = {"1H": 1, "4H": 4, "8H": 8, "12H": 12}
    hours_selected = hours_map[time_filter]
with col_ctrl2:
    st.empty()

# --- LIVE REFRESH FRAGMENT ---
@st.fragment(run_every=2)
def display_monitoring(hours):
    """
    Core Display Loop: Fetches, logs, and visualizes live system state.
    """
    m = get_metrics()
    
    # 1. Persistence Logic
    if "Running" in m['status']:
        try:
            ts = pd.to_datetime(m['read_time']).tz_convert('Asia/Kolkata').strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            ts = None
        log_metrics(m['cpu_pct'], m['mem_mb'], m['mem_pct'], ts)
    else:
        log_metrics(0, 0, 0)
    
    # 2. Layout Distribution
    col_chart, col_cards = st.columns([4, 1], gap="small")
    
    # --- CHART VISUALIZATION ---
    with col_chart:
        df = get_history(hours)
        if not df.empty:
            df = df.drop_duplicates('timestamp').sort_values('timestamp')
            
            fig = go.Figure()

            # Primary: Memory Usage (Area Chart)
            fig.add_trace(go.Scatter(
                x=df['timestamp'], y=df['mem_mb'], name='RAM (MB)',
                mode='lines', line=dict(color='#8b5cf6', width=2),
                fill='tozeroy', fillcolor='rgba(139, 92, 246, 0.1)',
                yaxis='y'
            ))

            # Secondary: CPU Load (Overlay)
            fig.add_trace(go.Scatter(
                x=df['timestamp'], y=df['cpu_pct'], name='CPU (%)',
                mode='lines', line=dict(color='#3b82f6', width=2),
                fill='tozeroy', fillcolor='rgba(59, 130, 246, 0.1)',
                yaxis='y2'
            ))

            # Layout Styling
            fig.update_layout(
                template="plotly_dark",
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                margin=dict(l=10, r=10, b=0, t=10),
                height=280,
                xaxis=dict(showgrid=False, type='date', tickformat='%H:%M:%S', tickfont=dict(size=10, color='#64748b')),
                yaxis=dict(
                    title=dict(text='RAM (MB)', font=dict(color='#8b5cf6', size=11)),
                    tickfont=dict(color='#8b5cf6', size=10),
                    showgrid=True, gridcolor='rgba(255,255,255,0.05)',
                    range=[0, m['mem_limit_num']]
                ),
                yaxis2=dict(
                    title=dict(text='CPU (%)', font=dict(color='#3b82f6', size=11)),
                    tickfont=dict(color='#3b82f6', size=10),
                    overlaying='y', side='right',
                    showgrid=False, range=[0, 100]
                ),
                showlegend=False,
                hovermode="x unified"
            )
            
            st.plotly_chart(fig, use_container_width=True, key="live_metrics")
        else:
            st.info("Synchronizing telemetry pipeline...")
            
    # --- STATUS CARDS ---
    with col_cards:
        # Cap visual width to 100% to prevent card breakage
        cpu_w = min(m['cpu_pct'], 100)
        mem_w = min(m['mem_pct'], 100)
        
        st.markdown(f"""
            <div class="resource-card-small">
                <div class="card-label">CPU Load</div>
                <div class="card-value-small">{m['cpu_pct']}%</div>
                <div class="progress-bar"><div class="progress-fill fill-blue" style="width: {cpu_w}%"></div></div>
                <div class="card-footer-small">
                    <span>{m['max_cores']} Capacity</span>
                    <span style="color: #3b82f6;">{m['status']}</span>
                </div>
            </div>
            
            <div class="resource-card-small">
                <div class="card-label">RAM Consumption</div>
                <div class="card-value-small">{m['mem_val']}</div>
                <div class="progress-bar"><div class="progress-fill fill-purple" style="width: {mem_w}%"></div></div>
                <div class="card-footer-small">
                    <span>{m['mem_limit']} Limit</span>
                    <span style="color: #8b5cf6;">({m['mem_pct']:.1f}% Used)</span>
                </div>
            </div>
        """, unsafe_allow_html=True)

# Start display execution
display_monitoring(hours_selected)
