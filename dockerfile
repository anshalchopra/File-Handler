# Docker Resource Sandbox for Stress Testing
FROM python:3.12-slim

# set working directory
WORKDIR /app

# Install only necessary libraries for CSV/JSON/API stress testing
RUN pip install --no-cache-dir streamlit pandas plotly fastapi uvicorn faker requests

# copy scripts into the container
COPY scripts /app/scripts
# create data directory
RUN mkdir -p /app/data

# expose streamlit and fast api port
EXPOSE 8501 8000

CMD ["bash", "/app/scripts/entrypoint.sh"]