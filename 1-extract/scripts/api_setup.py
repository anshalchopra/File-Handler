"""
Data Ingestion API
------------------
This module provides a FastAPI-based ingestion service to receive data 
from sensor simulations. It uses Pydantic models for data validation 
and ensures all incoming records adhere to the expected schema.
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import os
import sqlite3
from datetime import datetime

# Initialize FastAPI app
app = FastAPI(
    title="Sensor Ingestion API",
    description="Receives and validates sensor data for the extraction pipeline."
)

# --- DATA SCHEMA ---
# Pydantic's BaseModel handles automatic data validation and documentation
class SensorData(BaseModel):
    """
    Schema for a single sensor observation record.
    Matches the output of data_generator.py.
    """
    timestamp: str          # Format: YYYY-MM-DD HH:MM:SS
    ph_level: float         # 0.0 - 14.0
    ec_tds: float           # Electrical Conductivity
    water_temp: float       # In Celsius
    air_temp: float         # In Celsius
    humidity: int           # 0 - 100%
    water_level: int        # 0 - 100%

# --- DATABASE LOGIC ---
# In a real scenario, this would write to the same monitoring.db 
# used by the dashboard to show end-to-end data flow.
DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "monitoring.db"))

def save_to_db(data: SensorData):
    """
    Persists a single validated sensor record.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""
            INSERT INTO sensor_data (timestamp, ph_level, ec_tds, water_temp, air_temp, humidity, water_level) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (data.timestamp, data.ph_level, data.ec_tds, data.water_temp, data.air_temp, data.humidity, data.water_level))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Database insertion failed: {e}")

def save_bulk_to_db(records: List[SensorData]):
    """
    Persists a list of records using a single transaction.
    Highly efficient for batch operations.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        data_tuples = [
            (r.timestamp, r.ph_level, r.ec_tds, r.water_temp, r.air_temp, r.humidity, r.water_level) 
            for r in records
        ]
        c.executemany("""
            INSERT INTO sensor_data (timestamp, ph_level, ec_tds, water_temp, air_temp, humidity, water_level) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, data_tuples)
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Bulk database insertion failed: {e}")

# --- API ENDPOINTS ---

@app.post("/receive-data", status_code=201)
async def receive_data(record: SensorData):
    """
    Endpoint for sequential (stream) data ingestion.
    """
    save_to_db(record)
    return {"status": "success", "message": "Record ingested successfully"}

@app.post("/receive-bulk-data", status_code=201)
async def receive_bulk_data(records: List[SensorData]):
    """
    Endpoint for bulk-loaded (batch) data ingestion.
    """
    if records:
        save_bulk_to_db(records)
    return {
        "status": "success", 
        "count": len(records),
        "message": "Batch ingested successfully"
    }

if __name__ == "__main__":
    import uvicorn
    # Start the server locally for testing
    # Inside the container, this is handled by entrypoint.sh
    uvicorn.run(app, host="0.0.0.0", port=8000)
