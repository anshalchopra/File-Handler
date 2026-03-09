import streamlit as st
import sys
import os

# Set page config FIRST
st.set_page_config(
    page_title="Data Sandbox Ecosystem",
    page_icon="✨",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Add extract script directory to path for imports
base_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(base_path, "1-extract", "scripts"))

# Import the modular dashboards
try:
    import streamlit_dashboard as extract_dash
except ImportError:
    st.error("Could not import Extraction Dashboard. Ensure 1-extract/scripts/streamlit_dashboard.py exists.")

import importlib.util

def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

try:
    transform_dash = load_module("transform_dash", os.path.join(base_path, "2-transform", "scripts", "streamlit_dashboard.py"))
except Exception as e:
    transform_dash = None
    st.error(f"Could not load Transform Dashboard: {e}")

try:
    load_dash = load_module("load_dash", os.path.join(base_path, "3-load", "scripts", "streamlit_dashboard.py"))
except Exception as e:
    load_dash = None
    st.error(f"Could not load Load Dashboard: {e}")

# Create a container for the hero header to keep it consistent
st.markdown("""
<div class="hero-title">Data Sandbox Ecosystem</div>
<div class="hero-subtitle">Unified Real-time Monitoring & Diagnostics</div>
<div style="height: 2rem;"></div>
""", unsafe_allow_html=True)

# Define the tabs
tab_extract, tab_transform, tab_load = st.tabs(["🏗️ EXTRACT", "🔄 TRANSFORM", "📥 LOAD"])

with tab_extract:
    extract_db = os.path.join(base_path, "1-extract", "data", "monitoring.db")
    extract_dash.set_db_path(extract_db)
    extract_dash.render_dashboard(stage_name="Extract", api_port=8000)

with tab_transform:
    if transform_dash:
        transform_dash.render_dashboard(stage_name="Transform", api_port=8001)
    else:
        st.warning("Transform module unavailable.")

with tab_load:
    if load_dash:
        load_dash.render_dashboard(stage_name="Load", api_port=8002)
    else:
        st.warning("Load module unavailable.")

# Global styles derived from the glassmorphic theme
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@200;300;400;500;600&display=swap');
    
    .stApp {
        background-color: #0d0d0f;
        background-image: 
            radial-gradient(circle at 20% 20%, rgba(1, 124, 195, 0.15) 0%, transparent 40%),
            radial-gradient(circle at 80% 80%, rgba(173, 212, 229, 0.05) 0%, transparent 40%);
        color: #e2e2e7;
    }

    .hero-title {
        text-align: center;
        background: linear-gradient(135deg, #017CC3 0%, #ADD4E5 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 3.5rem;
        font-weight: 600;
        margin-top: 1rem;
        margin-bottom: 0.25rem;
        letter-spacing: -2px;
    }

    .hero-subtitle {
        text-align: center;
        color: #ADD4E5;
        font-size: 0.9rem;
        letter-spacing: 2px;
        text-transform: uppercase;
        margin-bottom: 1rem;
        opacity: 0.8;
    }

    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
        justify-content: center;
        background-color: transparent;
    }

    .stTabs [data-baseweb="tab"] {
        height: 50px;
        background-color: rgba(255, 255, 255, 0.03);
        border-radius: 8px 8px 0 0;
        padding: 0 30px;
        color: #ADD4E5;
        font-weight: 400;
        border: 1px solid rgba(173, 212, 229, 0.1);
        border-bottom: none;
    }

    .stTabs [aria-selected="true"] {
        background-color: rgba(1, 124, 195, 0.1) !important;
        border-top: 2px solid #017CC3 !important;
        color: white !important;
    }
</style>
""", unsafe_allow_html=True)
