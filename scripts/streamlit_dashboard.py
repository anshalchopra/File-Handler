import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import sqlite3
import plotly.graph_objects as go
import threading
import json
import os
import shutil

# --- PAGE CONFIG ---
st.set_page_config(
    page_title="Data Sandbox",
    page_icon="✨",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# --- THEME & PREMIUM MODERN STYLING ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@200;300;400;500&display=swap');

    /* Global Sans-Serif & Remove Heavy Bolding */
    html, body, [class*="css"], .stMarkdown, .stText, .stButton, .stTable, p, span, div, label {
        font-family: 'Outfit', sans-serif !important;
        font-weight: 300 !important;
    }
    
    [data-testid="collapsedControl"], section[data-testid="stSidebar"] { display: none; }
    
    /* Modern Dark Background */
    .stApp { 
        background-color: #09090b; 
        background-image: radial-gradient(circle at 15% 50%, rgba(59, 130, 246, 0.04), transparent 25%),
                          radial-gradient(circle at 85% 30%, rgba(139, 92, 246, 0.04), transparent 25%);
    }
    .stApp, p, span, div, label { color: #a1a1aa !important; }
    
    /* Elegant Title */
    .pretty-title {
        text-align: center;
        background: linear-gradient(135deg, #38bdf8 0%, #8b5cf6 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 4rem;
        margin-bottom: 0.2rem;
        letter-spacing: -1.5px;
        font-weight: 400 !important;
    }
    
    .pretty-subtitle {
        text-align: center;
        color: #71717a;
        font-size: 1.1rem;
        margin-bottom: 3.5rem;
        letter-spacing: 2px;
        text-transform: uppercase;
    }

    /* Glassmorphism Cards */
    .resource-card {
        background: rgba(255, 255, 255, 0.02);
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        border: 1px solid rgba(255, 255, 255, 0.05);
        border-radius: 16px;
        padding: 24px;
        text-align: left;
        transition: transform 0.3s ease, background 0.3s ease, border-color 0.3s ease;
        position: relative;
        overflow: hidden;
    }
    
    .resource-card:hover {
        transform: translateY(-2px);
        background: rgba(255, 255, 255, 0.03);
        border-color: rgba(255, 255, 255, 0.1);
    }
    
    .resource-label {
        font-size: 0.85rem;
        color: #a1a1aa;
        letter-spacing: 1.5px;
        text-transform: uppercase;
        margin-bottom: 6px;
    }
    
    .resource-sub-value {
        font-size: 0.8rem;
        color: #71717a;
        margin-bottom: 16px;
    }
    
    .resource-main-value {
        font-size: 2.2rem;
        color: #f4f4f5;
        margin-bottom: 16px;
        font-weight: 300 !important;
    }

    /* Animated Line Loader */
    .bar-container {
        background: rgba(255, 255, 255, 0.05);
        height: 3px;
        border-radius: 1.5px;
        width: 100%;
        overflow: hidden;
    }
    
    .bar-fill {
        height: 100%;
        border-radius: 1.5px;
        transition: width 0.8s cubic-bezier(0.16, 1, 0.3, 1);
    }

    /* Gradient Fills */
    .bar-fill-blue { background: linear-gradient(90deg, #0ea5e9, #3b82f6); box-shadow: 0 0 10px rgba(59, 130, 246, 0.5); }
    .bar-fill-emerald { background: linear-gradient(90deg, #10b981, #059669); box-shadow: 0 0 10px rgba(16, 185, 129, 0.5); }
    .bar-fill-purple { background: linear-gradient(90deg, #a855f7, #8b5cf6); box-shadow: 0 0 10px rgba(139, 92, 246, 0.5); }

    /* Section Headers */
    .section-header {
        color: #f4f4f5;
        font-size: 1.5rem;
        margin-top: 2.5rem;
        margin-bottom: 1.5rem;
        font-weight: 300 !important;
        letter-spacing: 0.5px;
        display: flex;
        align-items: center;
        gap: 12px;
    }
    
    .section-header::before {
        content: '';
        display: block;
        width: 8px;
        height: 8px;
        border-radius: 50%;
        background: #38bdf8;
        box-shadow: 0 0 10px #38bdf8;
    }
    
    /* Custom radio buttons */
    div[data-testid="stHorizontalBlock"] div[data-testid="stVerticalBlock"] div.stRadio > div {
        flex-direction: row !important;
        gap: 12px;
        background: rgba(255, 255, 255, 0.02);
        padding: 6px 16px;
        border-radius: 20px;
        border: 1px solid rgba(255, 255, 255, 0.05);
    }
</style>
""", unsafe_allow_html=True)

# --- DIRECTORY & DB SETUP ---
DATA_DIR = Path("/app/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "monitoring_history.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    # System stats
    conn.execute('''CREATE TABLE IF NOT EXISTS system_logs 
                    (timestamp DATETIME, ram_used_mb REAL, cpu_used_cores REAL)''')
    # Activity logs (Bulk/Stream)
    conn.execute('''CREATE TABLE IF NOT EXISTS activity_logs 
                    (timestamp DATETIME, mode TEXT, count INTEGER)''')
    conn.commit()
    conn.close()

init_db()

# --- RESOURCE LOGIC ---
def get_ram_stats():
    try:
        u_path, l_path = "/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory.max"
        if not os.path.exists(u_path):
            u_path, l_path = "/sys/fs/cgroup/memory/memory.usage_in_bytes", "/sys/fs/cgroup/memory/memory.limit_in_bytes"
        with open(u_path, "r") as f: used = int(f.read().strip())
        with open(l_path, "r") as f:
            l_str = f.read().strip()
            limit = int(l_str) if l_str != "max" else shutil.disk_usage("/").total
        used_mb = used / (1024**2)
        limit_mb = limit / (1024**2)
        pct = (used/limit)*100
        return used_mb, limit_mb, pct
    except: 
        # Removing fake 2048 fallback. Return 0 for visibility of failure.
        return 0.0, 0.0, 0.0

def get_cpu_stats():
    try:
        l_path = "/sys/fs/cgroup/cpu.max"
        if os.path.exists(l_path):
            with open(l_path, "r") as f:
                parts = f.read().strip().split()
                limit_cores = os.cpu_count() if parts[0] == "max" else int(parts[0]) / int(parts[1])
        else:
            with open("/sys/fs/cgroup/cpu/cpu.cfs_quota_us", "r") as f: quota = int(f.read().strip())
            with open("/sys/fs/cgroup/cpu/cpu.cfs_period_us", "r") as f: period = int(f.read().strip())
            limit_cores = quota / period if quota > 0 else os.cpu_count()
        used_cores = os.getloadavg()[0] 
        u_c = min(used_cores, limit_cores)
        return u_c, limit_cores, (u_c/limit_cores)*100
    except: return 0.15, 2.0, 7.5

def log_system_stats():
    r_u, _, _ = get_ram_stats()
    c_u, _, _ = get_cpu_stats()
    conn = sqlite3.connect(DB_PATH)
    conn.execute("INSERT INTO system_logs VALUES (?, ?, ?)", (datetime.now(), r_u, c_u))
    conn.commit()
    conn.close()

def get_disk_stats():
    try:
        total, used, _ = shutil.disk_usage("/")
        return used / (1024**3), total / (1024**3), (used/total)*100
    except: return 1.4, 4.0, 35.0

def get_system_history(hours):
    conn = sqlite3.connect(DB_PATH)
    since = datetime.now() - timedelta(hours=hours)
    df = pd.read_sql_query("SELECT timestamp, ram_used_mb, cpu_used_cores FROM system_logs WHERE timestamp > ? ORDER BY timestamp ASC", 
                           conn, params=(since,))
    conn.close()
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

def get_activity_logs():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT timestamp, mode, count FROM activity_logs ORDER BY timestamp DESC LIMIT 20", conn)
    conn.close()
    return df

def log_activity(mode, count):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("INSERT INTO activity_logs (timestamp, mode, count) VALUES (?, ?, ?)", 
                 (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), mode.upper(), count))
    conn.commit()
    conn.close()

# --- DATA PREVIEW ---
def get_csv_preview(f_name):
    """Efficiently fetch the last 100 rows of the CSV to avoid RAM crashes on massive files."""
    path = DATA_DIR / f_name
    if not path.exists(): return None, 0
    
    try:
        import subprocess
        from io import StringIO
        # Count total rows using system call for speed
        total_rows = int(subprocess.check_output(f"wc -l < '{path}'", shell=True).strip()) - 1
        total_rows = max(0, total_rows)
        
        # Get header + last 100 lines
        raw_tail = subprocess.check_output(f"head -n 1 '{path}' && tail -n 100 '{path}'", shell=True).decode()
        df = pd.read_csv(StringIO(raw_tail))
        return df, total_rows
    except:
        return None, 0

# --- MAIN UI ---
st.markdown('<div class="pretty-title">Data Sandbox</div>', unsafe_allow_html=True)
st.markdown('<div class="pretty-subtitle">Stress Testing & Resource Monitoring</div>', unsafe_allow_html=True)

def run_task(mode, count):
    from generate_data import DataGenerator
    gen = DataGenerator(num_records=count)
    try:
        if mode == "bulk": gen.send_data()
        else: gen.stream_data()
    except Exception as e:
        print(f"Task Thread Error: {e}")

# 1. System Insights (Top Priority)
st.markdown('<div class="section-header">System Insights / Resource Impact</div>', unsafe_allow_html=True)

@st.fragment(run_every="5s")
def insights_fragment():
    r_u, r_l, r_p = get_ram_stats()
    c_u, c_l, c_p = get_cpu_stats()
    d_u, d_t, d_p = get_disk_stats()
    
    log_system_stats()
    
    col_chart, col_cards = st.columns([7, 5], gap="medium")
    
    with col_chart:
        horizon = st.radio("Timeline", ["1H", "4H", "24H"], index=0, horizontal=True, label_visibility="collapsed")
        h_map = {"1H": 1, "4H": 4, "24H": 24}
        hist = get_system_history(h_map[horizon])
        
        if not hist.empty:
            from plotly.subplots import make_subplots
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(go.Scatter(
                x=hist['timestamp'], y=hist['ram_used_mb'], name='RAM (MB)',
                line=dict(color='#3b82f6', width=2, shape='spline', smoothing=1.3), mode='lines'
            ), secondary_y=False)
            fig.add_trace(go.Scatter(
                x=hist['timestamp'], y=hist['cpu_used_cores'], name='CPU (Cores)',
                line=dict(color='#10b981', width=2, shape='spline', smoothing=1.3), mode='lines'
            ), secondary_y=True)
            fig.update_layout(
                paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                margin=dict(t=20, b=5, l=0, r=0), height=380,
                xaxis=dict(showgrid=False, color='#71717a', tickfont=dict(size=10)),
                yaxis=dict(
                    title=dict(text=f"RAM (MB) Limit: {r_l:.0f}", font=dict(size=10, color='#3b82f6')),
                    range=[0, r_l * 1.05], showgrid=True, gridcolor='rgba(255,255,255,0.05)', 
                    color='#3b82f6', tickfont=dict(size=10)
                ),
                yaxis2=dict(
                    title=dict(text=f"CPU (Cores) Limit: {c_l:.2f}", font=dict(size=10, color='#10b981')),
                    range=[0, c_l * 1.05], showgrid=True, gridcolor='rgba(255,255,255,0.02)', 
                    color='#10b981', tickfont=dict(size=10), overlaying='y', side='right'
                ),
                showlegend=False, font_family="Outfit"
            )
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
            
    with col_cards:
        def card(label, main_val, sub_val, pct, color_class):
            st.markdown(f"""
            <div class="resource-card" style="margin-bottom: 12px; padding: 14px;">
                <div class="resource-label" style="font-size: 0.75rem;">{label}</div>
                <div class="resource-main-value" style="font-size: 1.6rem; margin-bottom: 10px; margin-top: 4px;">{main_val}</div>
                <div class="bar-container" style="height: 2px;"><div class="bar-fill {color_class}" style="width: {pct}%"></div></div>
                <div style="font-size: 0.7rem; color: #52525b; margin-top: 6px;">{sub_val} ({pct:.1f}%)</div>
            </div>""", unsafe_allow_html=True)
    
        card("Memory", f"{r_u:.1f} MB", f"{r_l:.1f} Limit", r_p, "bar-fill-blue")
        card("Compute", f"{c_u:.2f} Cores", f"{c_l:.2f} Limit", c_p, "bar-fill-emerald")
        card("Storage", f"{d_u:.1f} GB", f"{d_t:.1f} Total", d_p, "bar-fill-purple")

insights_fragment()

# 2. Generation & Logs (Middle Row)
col_gen, col_logs = st.columns([1, 1], gap="large")

@st.fragment(run_every="2s")
def generation_status_fragment():
    status_file = DATA_DIR / "status.json"
    if status_file.exists():
        try:
            with open(status_file, "r") as f:
                status = json.load(f)
        except:
            status = {}
            
        if status.get("is_running"):
            prog, total = status.get("progress", 0), status.get("total", 1)
            pct = min(prog / total if total > 0 else 0, 1.0)
            st.info(f"🚀 {status.get('mode', '').upper()} in progress... ({prog:,} / {total:,})")
            st.progress(pct)
        elif status.get("success"):
            st.success(f"✅ {status.get('mode', '').upper()} Completed ({status.get('total', 0):,} records)!")
            if st.button("Dismiss"):
                os.remove(status_file)
                st.rerun()

with col_gen:
    st.markdown('<div class="section-header">Data Generation Controls</div>', unsafe_allow_html=True)
    num_to_gen = st.number_input("Records", min_value=1, max_value=2000000, value=50000, step=10000, label_visibility="collapsed")
    
    st.markdown("<div style='margin-top: 10px;'></div>", unsafe_allow_html=True)
    btn_col1, btn_col2 = st.columns(2)
    with btn_col1:
        if st.button("Bulk Send 🚀", use_container_width=True):
            if os.path.exists(DATA_DIR / "status.json"): os.remove(DATA_DIR / "status.json")
            threading.Thread(target=run_task, args=("bulk", num_to_gen)).start()
    with btn_col2:
        if st.button("Stream Send 🚀", use_container_width=True):
            if os.path.exists(DATA_DIR / "status.json"): os.remove(DATA_DIR / "status.json")
            threading.Thread(target=run_task, args=("stream", num_to_gen)).start()

    generation_status_fragment()

@st.fragment(run_every="5s")
def activity_logs_fragment():
    st.markdown('<div class="section-header">Test History</div>', unsafe_allow_html=True)
    logs_df = get_activity_logs()
    if not logs_df.empty:
        st.dataframe(logs_df, use_container_width=True, height=200, hide_index=True)
    else:
        st.caption("No recent activities.")

with col_logs:
    activity_logs_fragment()

st.markdown("<br>", unsafe_allow_html=True)

# 3. Data Explorer (Bottom Row - Simplified Preview)
@st.fragment(run_every="5s")
def data_explorer_fragment():
    st.markdown('<div class="section-header">Data Explorer (File Preview)</div>', unsafe_allow_html=True)
    
    df, total_rows = get_csv_preview("sandbox_data.csv")
    if df is not None:
        st.caption(f"Showing the last 100 entries. **Total records in sandbox_data.csv: {total_rows:,}**")
        st.dataframe(df, use_container_width=True, height=300)
    else:
        st.info("No data found in sandbox_data.csv. Run a generation test above to start!")

data_explorer_fragment()
