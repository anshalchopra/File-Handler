"""
Data Sandbox Dashboard
----------------------
A high-performance Streamlit dashboard for real-time monitoring of 
sandbox resource utilization. Features live telemetry, historical 
data visualization, and system diagnostics.
"""

import streamlit as st
import psutil
import os
import time
import sqlite3
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
import docker
import sys

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
    
    # Perform resilient migration for the mem_pct column if adding to an older schema
    try:
        c.execute("ALTER TABLE metrics ADD COLUMN mem_pct REAL")
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

    /* --- SIDEBAR CUSTOMIZATION --- */
    section[data-testid="stSidebar"] {
        background-color: #09090b !important;
        border-right: 1px solid rgba(255, 255, 255, 0.05);
    }

    .sidebar-header {
        font-size: 1rem;
        font-weight: 600;
        color: #f8fafc;
        margin-bottom: 1rem;
        margin-top: 0.5rem;
        display: flex;
        align-items: center;
        gap: 8px;
        opacity: 0.9;
    }

    .sidebar-section {
        background: rgba(255, 255, 255, 0.015);
        border: 1px solid rgba(255, 255, 255, 0.04);
        border-radius: 10px;
        padding: 0.8rem 1rem;
        margin-bottom: 1rem;
    }

    .sidebar-label {
        font-size: 0.75rem;
        color: #94a3b8;
        font-weight: 500;
        margin-bottom: 0.5rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }

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
</style>
""", unsafe_allow_html=True)

# Initialize system data
init_db()

# --- HEADER SECTION ---
st.markdown('<h1 class="hero-title">Data Sandbox</h1>', unsafe_allow_html=True)
st.markdown('<p class="hero-subtitle">Host-Based Sandbox Monitoring</p>', unsafe_allow_html=True)

# --- SIDEBAR & DIAGNOSTICS ---
with st.sidebar:
    st.markdown('<div class="sidebar-header">🛠️ Diagnostics</div>', unsafe_allow_html=True)
    
    st.markdown('<div class="sidebar-section">', unsafe_allow_html=True)
    st.markdown('<div class="sidebar-label">System Testing</div>', unsafe_allow_html=True)
    if st.button('🔥 Stress Test (15s)', use_container_width=True):
        try:
            # Connect to Docker and initiate high-load process
            try:
                client = docker.from_env()
                client.ping()
            except Exception:
                client = docker.DockerClient(base_url="unix:///var/run/docker.sock")
                
            container = client.containers.get("sandbox")
            container.exec_run("sh -c 'timeout 15s python3 -c \"while True: pass\"'", detach=True)
            st.toast("🔥 Load simulation active", icon="⚡")
        except Exception:
            st.error("Sandbox unreachable")
    st.markdown('</div>', unsafe_allow_html=True)

# --- TELEMETRY LOGIC ---
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
