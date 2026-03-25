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
ingestion_queue = asyncio.Queue()

# --- 3. BACKGROUND WORKER (THE ENGINE) ---
async def database_worker():
    """
    High-Speed Ingestion Engine:
    Gathers records in memory and performs a single bulk insert periodically.
    """
    batch = []
    MAX_BATCH_SIZE = 1000
    FLUSH_INTERVAL = 1.0

    while True:
        try:
            # 1. Wave-Collect: Try to gather many records within the FLUSH_INTERVAL
            try:
                if not batch:
                    # Wait for the first record indefinitely to save idle CPU
                    record = await ingestion_queue.get()
                    batch.append(record)
                
                # After the first record, try to pack as many as possible within the interval
                while len(batch) < MAX_BATCH_SIZE:
                    # Wait for next records but don't block past the flush interval
                    record = await asyncio.wait_for(ingestion_queue.get(), timeout=FLUSH_INTERVAL)
                    batch.append(record)
            except (asyncio.TimeoutError, TimeoutError):
                pass # Trigger flush after interval

            # 2. Database Flush
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
            logger.error(f"❌ Worker DB Error: {repr(e)}")
            batch.clear()
            await asyncio.sleep(1) # Grace period before retry

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
    Response time: < 1 millisecond.
    """
    # Simply put the data in the queue for the background worker to handle
    await ingestion_queue.put(record)
    return {"status": "success", "info": "Record queued for asynchronous batch insertion"}

@app.post("/receive-bulk-data", status_code=201)
async def receive_bulk_data(records: List[SensorData]):
    """
    BATCH ENDPOINT: Drops a massive list of readings directly into the queue.
    """
    for record in records:
        await ingestion_queue.put(record)
            
    return {"status": "success", "count": len(records), "info": "Bulk records queued"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api_setup:app", host="0.0.0.0", port=8000, reload=True)