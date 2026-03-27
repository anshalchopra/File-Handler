#!/bin/bash

# --- CONTAINER ENTRYPOINT SCRIPT ---
# This script manages the lifecycle of services inside the Docker sandbox.
# It ensures that monitoring and processing tasks are started in the correct order.

# 1. API Initialization
# The API serves as the ingestion point for generated sensor data.
echo "Starting Data Ingestion API on port 9000..."
python3 scripts/api_setup.py &

# 2. Monitoring Dashboard Launch
# Streamlit provides the visual front-end for resource and process telemetry.
# We bind it to 0.0.0.0 to allow access from the host machine via mapped ports.
echo "🚀 Initializing Streamlit Dashboard [Port 9501]..."
streamlit run scripts/streamlit_dashboard.py --server.port 9501 --server.address 0.0.0.0

# Keep the shell active if needed (though Streamlit usually captures the foreground)
wait
