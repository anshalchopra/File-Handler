"""
🚀 High-Performance Async Data Generator
----------------------------------------
This engine simulates a swarm of sensors by combining:
1. Multiprocessing: To utilize multiple CPU cores for heavy record generation.
2. AsyncIO/httpx: To 'blast' thousands of records to the API at the same time.

Structure:
- start_generating: Launches the OS-level worker processes.
- stream_data / batch_data: Orchestrates the AsyncIO blast inside each process.
"""

import multiprocessing
import os
import random
import httpx
import json
import asyncio
import logging
from datetime import datetime
from typing import Optional

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
logger = logging.getLogger("Generator")

# Mute noisy network library logs
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# --- 2. DATA GENERATION CLASS ---
class GenerateData:
    """
    Handles the generation and high-speed transmission of sensor data.
    """
    def __init__(self, 
                 num_records: int = 500, 
                 num_process: int = 1, 
                 method: str = "stream", 
                 port: int = 8000, 
                 num_batches: Optional[int] = 1):
        
        self.num_record = num_records
        self.num_process = num_process
        self.method = method
        self.port = port
        self.num_batches = num_batches
        
        # API Target Endpoints
        self.stream_url = f"http://localhost:{port}/receive-data"
        self.batch_url = f"http://localhost:{port}/receive-bulk-data"
        
        # Fallback directory if the API is down
        self.data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
    # --- 2. FAST RECORD POOLING ---
    def _create_record_batch(self, size: int) -> list:
        """
        PRE-CALC: Generates a batch of records 10x faster than 1-by-1 math.
        """
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return [{ 
            "timestamp": ts,
            "ph_level": round(random.uniform(5.5, 6.5), 2),
            "ec_tds": round(random.uniform(1.2, 2.5), 2),
            "water_temp": round(random.uniform(18.0, 22.0), 1),
            "air_temp": round(random.uniform(20.0, 28.0), 1),
            "humidity": random.randint(40, 70),
            "water_level": random.randint(70, 100)
        } for _ in range(size)]

    # --- 3. DISK LOGGING (WITH to_thread) ---
    async def _save_to_disk(self, data, error_reason="Unknown Error"):
        """
        DISK-FALLBACK: Saves records to a file if the server is offline.
        """
        os.makedirs(self.data_dir, exist_ok=True)
        filepath = os.path.join(self.data_dir, "failed_transmissions.json")
        
        log_entry = {
            "captured_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "method": self.method,
            "error_reason": str(error_reason),
            "data": data
        }

        try:
            def write_file():
                with open(filepath, 'a') as f:
                    f.write(json.dumps(log_entry) + "\n")
            
            await asyncio.to_thread(write_file)
            print(f"💾 Server Offline: Data cached locally.")
        except Exception as e:
            print(f"❌ Disk Error: {e}")

    # --- 4. ASYNC TRANSMISSION HELPER ---
    async def _send_record(self, client: httpx.AsyncClient, record: dict):
        """
        NON-BLOCKING: Sends one record and 'waits' for the network asynchronously.
        """
        try:
            response = await client.post(self.stream_url, json=record)
            response.raise_for_status()
        except Exception as e:
            print(f"⚠️ Network Warning: {e}")
            await self._save_to_disk(record, error_reason=str(e))

    # --- 5. EXECUTION STRATEGIES (THE FIREHOSE) ---

    async def stream_data(self, count: int):
        pid = os.getpid()
        sent = 0
        
        sem = asyncio.Semaphore(30)
        limits = httpx.Limits(max_connections=150, max_keepalive_connections=30)
        timeout = httpx.Timeout(30.0)
        transport = httpx.AsyncHTTPTransport(limits=limits)
        
        async def sem_send(client, record):
            async with sem:
                try:
                    r = await client.post(self.stream_url, json=record)
                    r.raise_for_status()
                except: pass

        async with httpx.AsyncClient(transport=transport, timeout=timeout) as client:
            while sent < count:
                target_wave = max(5000, int(count * 0.10))
                wave_size = min(target_wave, 10000, count - sent)
                
                # BATCH GENERATION (FAST)
                wave_records = self._create_record_batch(wave_size)
                tasks = [sem_send(client, r) for r in wave_records]
                
                await asyncio.gather(*tasks)
                sent += wave_size
                #await asyncio.sleep(0.02) # Micro-rest to let API flush
        
        print(f"✨ [Process {pid}] Wave stream completed.")

    async def batch_data(self, count: int, num_batches: int = 1):
        """
        BULK UPLOAD: Groups records into large chunks and uploads them collectively.
        """
        pid = os.getpid()
        print(f"📦 [Process {pid}] Building {count} records into {num_batches} batches...")
        
        # INTEGRATION 2 (Part A): Fix math bug for batch remainders
        batch_size = count // num_batches
        remainder = count % num_batches
        
        # MEMORY FIX: Throttled batch shipping
        sem = asyncio.Semaphore(5) # Max 5 concurrent batch uploads
        limits = httpx.Limits(max_connections=50, max_keepalive_connections=5)
        transport = httpx.AsyncHTTPTransport(limits=limits)

        async with httpx.AsyncClient(transport=transport) as client:
            tasks = []
            for i in range(num_batches):
                current_batch_size = batch_size + (remainder if i == num_batches - 1 else 0)
                batch_records = self._create_record_batch(current_batch_size)
                
                async def send_batch_sem(data, idx):
                    async with sem:
                        try:
                            print(f"🚛 [Process {pid}] Shipping batch {idx+1} ({len(data)} records)...")
                            resp = await client.post(self.batch_url, json=data)
                            resp.raise_for_status()
                        except Exception as e:
                            print(f"⚠️ Batch Error: {e}")
                            await self._save_to_disk(data, error_reason=str(e))
                
                tasks.append(send_batch_sem(batch_records, i))
            
            await asyncio.gather(*tasks)

    # --- 6. MULTIPROCESSING ORCHESTRATOR ---

    def _worker_entrypoint(self, count: int, mode: str, batches: int):
        """
        BRIDGE: This is where each separate CPU process enters its own Event Loop.
        """
        if mode == "batch":
            asyncio.run(self.batch_data(count, batches))
        else:
            asyncio.run(self.stream_data(count))

    def start_generating(self):
        """
        COMMAND CENTER: Divides total work among your computer's CPU cores.
        """
        # INTEGRATION 2 (Part B): Fix math bug for process remainders
        records_per_process = self.num_record // self.num_process
        remainder = self.num_record % self.num_process
        
        processes = []

        print(f"🛠️  Initializing '{self.method.upper()}' engine with {self.num_process} worker(s)...")

        for i in range(self.num_process):
            # Ensure the last process picks up any remaining records
            worker_count = records_per_process + (remainder if i == self.num_process - 1 else 0)
            
            p = multiprocessing.Process(
                target=self._worker_entrypoint, 
                args=(worker_count, self.method, self.num_batches)
            )
            p.start()
            processes.append(p)

        for p in processes:
            p.join()
            
        print(f"✅ {self.method.upper()} extraction pipeline completed.")

# --- 7. MAIN START ---
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Data Generator Firehose")
    parser.add_argument("--records", type=int, default=500)
    parser.add_argument("--processes", type=int, default=1)
    parser.add_argument("--method", type=str, default="stream")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--batches", type=int, default=1)
    args = parser.parse_args()

    generator = GenerateData(
        num_records=args.records,
        num_process=args.processes,
        method=args.method,
        port=args.port,
        num_batches=args.batches
    )
    generator.start_generating()