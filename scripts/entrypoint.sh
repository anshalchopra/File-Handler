#!/bin/bash

# Start FastAPI in the background
echo "Starting FastAPI on port 8000..."
python scripts/api_setup.py &

# Start Streamlit in the foreground
echo "Starting Streamlit on port 8501..."
streamlit run scripts/streamlit_dashboard.py --server.port=8501 --server.address=0.0.0.0
