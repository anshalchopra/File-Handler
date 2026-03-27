import os
import sqlite3
import logging
from typing import List
from fastapi import FastAPI
from pydantic import BaseModel

DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "monitoring.db"))

class SensorData(BaseModel):
    timestamp: str
    ph_level: float
    ec_tds: float
    water_temp: float
    air_temp: float
    humidity: int
    water_level: int

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    c = conn.cursor()
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("""CREATE TABLE IF NOT EXISTS sensor_data (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, ph_level REAL, ec_tds REAL, water_temp REAL, air_temp REAL, humidity INTEGER, water_level INTEGER)""")
    c.execute("""CREATE TABLE IF NOT EXISTS metrics (timestamp DATETIME, cpu_pct REAL, mem_mb REAL, mem_pct REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS execution_logs (timestamp DATETIME, end_time DATETIME, method TEXT, records INTEGER, processes INTEGER, batches INTEGER, status TEXT)""")
    conn.commit()
    conn.close()

app = FastAPI(title="Synchronous Sensor Ingestion API")
init_db()

@app.post("/receive-data", status_code=201)
def receive_data(record: SensorData):
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        c = conn.cursor()
        c.execute("INSERT INTO sensor_data (timestamp, ph_level, ec_tds, water_temp, air_temp, humidity, water_level) VALUES (?,?,?,?,?,?,?)",
                  (record.timestamp, record.ph_level, record.ec_tds, record.water_temp, record.air_temp, record.humidity, record.water_level))
        conn.commit()
        conn.close()
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/receive-bulk-data", status_code=201)
def receive_bulk_data(records: List[SensorData]):
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        c = conn.cursor()
        data_tuples = [(r.timestamp, r.ph_level, r.ec_tds, r.water_temp, r.air_temp, r.humidity, r.water_level) for r in records]
        c.executemany("INSERT INTO sensor_data (timestamp, ph_level, ec_tds, water_temp, air_temp, humidity, water_level) VALUES (?,?,?,?,?,?,?)", data_tuples)
        conn.commit()
        conn.close()
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=9000)
