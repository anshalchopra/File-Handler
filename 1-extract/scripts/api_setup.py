"""
🚀 High-Performance Async Data Ingestion API
--------------------------------------------
Architecture Upgrades:
1. Persistent DB Connection: Eliminates the overhead of opening/closing files.
2. WAL Mode: Enables concurrent reads while writing.
3. Queue & Worker Pattern: Endpoints return instantly; a background worker batches DB writes.
"""

from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from pydantic import BaseModel
from typing import List
import os
import aiosqlite
import asyncio
import logging

# --- 1. LOGGING CONFIG ---
log_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "logs", "pipeline.log"))
os.makedirs(os.path.dirname(log_path), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_path),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("API")

# Mute noisy network and server logs for high-volume pipelines
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# --- 1. DEFINE DATA SCHEMA ---
class SensorData(BaseModel):
    timestamp: str
    ph_level: float
    ec_tds: float
    water_temp: float
    air_temp: float
    humidity: int
    water_level: int

# --- 2. GLOBAL STATE & CONFIG ---
DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "monitoring.db"))

# Global variables for the persistent connection and the ingestion queue
db_conn = None
ingestion_queue = asyncio.Queue(maxsize=1000000) # Gigantic queue for bulk stress tests

# --- 3. BACKGROUND WORKER (THE ENGINE) ---
async def database_worker():
    """
    High-Speed Ingestion Engine:
    Gathers records in memory and performs a single bulk insert periodically.
    """
    batch = []
    MAX_BATCH_SIZE = 5000
    FLUSH_INTERVAL = 0.1

    while True:
        try:
            # 1. High-Speed Intake
            record = await ingestion_queue.get()
            batch.append(record)
            
            # GHOST-BREATHE: Only sleep if the queue is shallow. 
            # If a tsunami of data is already here, skip the sleep and VACCUUM!
            if ingestion_queue.qsize() < 1000:
                await asyncio.sleep(FLUSH_INTERVAL)
            
            # 2. Fast-Grabbing: Drain the bucket at light speed
            while len(batch) < MAX_BATCH_SIZE:
                try:
                    record = ingestion_queue.get_nowait()
                    batch.append(record)
                except asyncio.QueueEmpty:
                    break
                    
            # 3. Database Flush (THE BULK STRIKE)
            if batch:
                data_tuples = [
                    (r.timestamp, r.ph_level, r.ec_tds, r.water_temp, r.air_temp, r.humidity, r.water_level)
                    for r in batch
                ]
                await db_conn.executemany("""
                    INSERT INTO sensor_data (timestamp, ph_level, ec_tds, water_temp, air_temp, humidity, water_level) 
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, data_tuples)
                await db_conn.commit()
                
                # Signal completion for metrics/logs
                for _ in range(len(batch)):
                    ingestion_queue.task_done()
                
                logger.info(f"💾 Flush Complete: {len(batch)} records committed to monitoring.db")
                batch.clear()

        except asyncio.CancelledError:
            # Shutdown sequence
            break
        except Exception as e:
            msg = f"❌ [CRITICAL] Worker DB Error: {repr(e)}"
            print(msg) # Hit the STDOUT directly
            logger.error(msg)
            batch.clear()
            await asyncio.sleep(2) # Grace period before retry - IT WILL RECOVER

# --- 4. LIFESPAN MANAGER (STARTUP/SHUTDOWN) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Replaces @app.on_event. Safely manages the database connection and background worker.
    """
    global db_conn
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    # Open ONE persistent connection
    db_conn = await aiosqlite.connect(DB_PATH)
    await db_conn.execute("PRAGMA journal_mode=WAL")
    await db_conn.commit()
    
    # Enable WAL mode for high concurrency
    await db_conn.execute("PRAGMA journal_mode=WAL;")
    await db_conn.execute("PRAGMA synchronous=NORMAL;")
    
    # Initialize Schema
    await db_conn.execute("""
        CREATE TABLE IF NOT EXISTS sensor_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT, ph_level REAL, ec_tds REAL, 
            water_temp REAL, air_temp REAL, humidity INTEGER, water_level INTEGER
        )
    """)
    await db_conn.commit()

    # Start the background database worker
    worker_task = asyncio.create_task(database_worker())
    
    yield # --- THIS IS WHERE THE API ACTUALLY RUNS ---

    # Shutdown sequence
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass
    await db_conn.close()

# --- 5. INITIALIZE API ---
app = FastAPI(
    title="Async Sensor Ingestion API",
    description="Designed for high-throughput sensor data ingestion.",
    lifespan=lifespan
)

# --- 6. API ENDPOINTS (THE INTAKE VALVES) ---

@app.post("/receive-data", status_code=201)
async def receive_data(record: SensorData):
    """
    STREAMING ENDPOINT: Instantly drops the record in memory and returns.
    """
    # Visibility Pulse
    if ingestion_queue.qsize() % 1000 == 0:
        print(f"📡 [Stream Intake] Queue Depth: {ingestion_queue.qsize()}")
        
    ingestion_queue.put_nowait(record)
    return {"status": "success", "info": "Record queued"}

@app.post("/receive-bulk-data", status_code=201)
async def receive_bulk_data(records: List[SensorData]):
    """
    BATCH ENDPOINT: Drops a massive list of readings directly into the queue.
    """
    count = len(records)
    logger.info(f"🌊 [Bulk Intake] Receiving {count} records into the memory pipe...")
    
    for record in records:
        ingestion_queue.put_nowait(record)
            
    return {"status": "success", "count": count, "info": "Bulk records queued"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api_setup:app", host="0.0.0.0", port=8000, reload=True)