import streamlit as st

def set_db_path(path):
    pass  # Placeholder, can be used later to point to a specific database

def render_dashboard(stage_name="Transform", api_port=8001):
    st.markdown(f"## 🔄 {stage_name} Pipeline Dashboard")
    
    st.markdown('''
        <style>
            .basic-card {
                background: rgba(255, 255, 255, 0.05);
                border: 1px solid rgba(173, 212, 229, 0.2);
                border-radius: 12px;
                padding: 20px;
                margin-bottom: 20px;
                text-align: center;
            }
        </style>
    ''', unsafe_allow_html=True)
    
    st.markdown(f'''
        <div class="basic-card">
            <h3>Welcome to the {stage_name} Stage</h3>
            <p>This is a basic placeholder dashboard. Later, you can add 
               complex transformation logic, data cleaning metrics, and 
               processing workflows here.</p>
            <p><strong>Configured API Port:</strong> {api_port}</p>
        </div>
    ''', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    with col1:
        st.info("📊 Data Transformation Stats will appear here.")
    with col2:
        st.success("✅ Transformation Rules Engine will be configurable here.")

if __name__ == "__main__":
    st.set_page_config(page_title="Transform Dashboard", page_icon="🔄", layout="wide")
    render_dashboard()
