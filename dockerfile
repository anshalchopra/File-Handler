# python slim image
FROM python:3.12-slim

# set working directory
WORKDIR /app

# install dependencies
RUN pip install --no-cache-dir streamlit pandas plotly openpyxl fastavro pyarrow fastapi uvicorn faker requests

# copy scripts into the container
COPY scripts /app/scripts
# create data directory
RUN mkdir -p /app/data

# expose streamlit and fast api port
EXPOSE 8501 8000

CMD ["bash", "/app/scripts/entrypoint.sh"]