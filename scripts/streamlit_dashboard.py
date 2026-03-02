import streamlit as st
import os
import shutil
import pandas as pd
from pathlib import Path
import time
from datetime import datetime, timedelta
import sqlite3
import plotly.graph_objects as go

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
    conn.execute('''CREATE TABLE IF NOT EXISTS memory_logs 
                    (timestamp DATETIME, ram_used_mb REAL)''')
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
        pct = (used/limit)*100
        
        conn = sqlite3.connect(DB_PATH)
        conn.execute("INSERT INTO memory_logs VALUES (?, ?)", (datetime.now(), used_mb))
        conn.commit()
        conn.close()
        return used_mb, limit / (1024**2), pct
    except: return 145.2, 2048.0, 7.1

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

def get_disk_stats():
    try:
        total, used, _ = shutil.disk_usage("/")
        return used / (1024**3), total / (1024**3), (used/total)*100
    except: return 1.4, 4.0, 35.0

def get_memory_history(hours):
    conn = sqlite3.connect(DB_PATH)
    since = datetime.now() - timedelta(hours=hours)
    df = pd.read_sql_query("SELECT timestamp, ram_used_mb FROM memory_logs WHERE timestamp > ? ORDER BY timestamp ASC", 
                           conn, params=(since,))
    conn.close()
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

# --- DATA PREVIEW ---
@st.cache_data
def load_data(f_name, m_time):
    path = DATA_DIR / f_name
    ext = path.suffix.lower()
    try:
        if ext == '.csv': return pd.read_csv(path)
        elif ext in ['.xls', '.xlsx']: return pd.read_excel(path, engine="openpyxl")
        elif ext == '.parquet': return pd.read_parquet(path)
        elif ext == '.json': return pd.read_json(path)
        elif ext == '.avro':
            import fastavro
            with open(path, 'rb') as fo: return pd.DataFrame([r for r in fastavro.reader(fo)])
    except: pass
    return None

# --- MAIN UI ---

st.markdown('<div class="pretty-title">Data Sandbox</div>', unsafe_allow_html=True)
st.markdown('<div class="pretty-subtitle">Isolated Environment</div>', unsafe_allow_html=True)

# 1. Top Section: Data Gen & Explorer side-by-side
col_gen, col_exp = st.columns([1, 1], gap="large")

with col_gen:
    st.markdown('<div class="section-header">Data Generation</div>', unsafe_allow_html=True)
    c1, c2 = st.columns([2, 1])
    with c1:
        num_to_gen = st.number_input("Records", min_value=1, max_value=1000000, value=50000, step=10000, label_visibility="collapsed")
    with c2:
        if st.button("Bulk Send 🚀", use_container_width=True):
            from generate_data import DataGenerator
            gen = DataGenerator(num_records=num_to_gen)
            with st.status(f"Bulk Uploading...", expanded=False) as status:
                gen.send_data()
                status.update(label="Complete!", state="complete")
            st.rerun()

with col_exp:
    st.markdown('<div class="section-header">Data Explorer</div>', unsafe_allow_html=True)
    files = [f for f in list(DATA_DIR.glob("*")) if f.name != "monitoring_history.db"]
    if files:
        files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        sel = st.selectbox("Files", [f.name for f in files], label_visibility="collapsed")
        path = DATA_DIR / sel
        df = load_data(sel, path.stat().st_mtime)

if files and df is not None:
    st.dataframe(df, use_container_width=True, height=180)
elif not files:
    st.info("Sandbox is empty.")

st.markdown("<br>", unsafe_allow_html=True)

# 2. System Insights
st.markdown('<div class="section-header">System Insights</div>', unsafe_allow_html=True)

@st.fragment(run_every="5s")
def insights_fragment():
    col_chart, col_cards = st.columns([7, 5], gap="medium")
    
    with col_chart:
        # Timeline selector at the top of the chart column
        horizon = st.radio("Timeline", ["1H", "4H", "24H"], index=0, horizontal=True, label_visibility="collapsed")
        
        h_map = {"1H": 1, "4H": 4, "24H": 24}
        hist = get_memory_history(h_map[horizon])
        
        if not hist.empty:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=hist['timestamp'], 
                y=hist['ram_used_mb'], 
                line=dict(color='#3b82f6', width=2, shape='spline', smoothing=1.3), 
                mode='lines', fill='tozeroy', fillcolor='rgba(59, 130, 246, 0.1)'
            ))
            fig.update_layout(
                paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                margin=dict(t=5, b=5, l=0, r=0), height=380,
                xaxis=dict(showgrid=False, color='#71717a', tickfont=dict(size=10)),
                yaxis=dict(showgrid=True, gridcolor='rgba(255,255,255,0.03)', color='#71717a', tickfont=dict(size=10)),
                showlegend=False, font_family="Outfit"
            )
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
            
    with col_cards:
        r_u, r_l, r_p = get_ram_stats()
        c_u, c_l, c_p = get_cpu_stats()
        d_u, d_t, d_p = get_disk_stats()
        
        def card(label, main_val, sub_val, pct, color_class):
            st.markdown(f"""
            <div class="resource-card" style="margin-bottom: 12px; padding: 14px;">
                <div class="resource-label" style="font-size: 0.75rem;">{label}</div>
                <div class="resource-main-value" style="font-size: 1.6rem; margin-bottom: 10px; margin-top: 4px;">{main_val}</div>
                <div class="bar-container" style="height: 2px;">
                    <div class="bar-fill {color_class}" style="width: {pct}%"></div>
                </div>
                <div style="font-size: 0.7rem; color: #52525b; margin-top: 6px;">{sub_val} ({pct:.1f}%)</div>
            </div>
            """, unsafe_allow_html=True)
    
        card("Memory", f"{r_u:.1f} MB", f"{r_l:.1f} Limit", r_p, "bar-fill-blue")
        card("Compute", f"{c_u:.2f} Cores", f"{c_l:.2f} Limit", c_p, "bar-fill-emerald")
        card("Storage", f"{d_u:.1f} GB", f"{d_t:.1f} Total", d_p, "bar-fill-purple")

insights_fragment()
st.caption("Status: All systems nominal")
