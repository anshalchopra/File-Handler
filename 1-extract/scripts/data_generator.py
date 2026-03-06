"""
Multi-Process Data Generation Engine
------------------------------------
This module simulates sensor data extraction. It generates synthetic 
resource/environment data and transmits it to the monitoring endpoints 
using parallel processing and multiple transmission strategies.
"""

import multiprocessing
import os
import random
import requests
import json
from datetime import datetime
from typing import Optional
from concurrent.futures import ThreadPoolExecutor

class GenerateData:
    """
    Handles the generation and transmission of synthetic sensor data.
    
    Features:
    - Multi-process distribution for high-frequency extraction.
    - Threaded IO for efficient 'stream' transmissions.
    - Grouped 'batch' transmission for bulk data transfers.
    - Disk-based fallback logging for data persistence during outages.
    """
    def __init__(self, 
                 num_records: int = 500, 
                 num_process: int = 1, 
                 method: str = "stream", 
                 port: int = 8000, 
                 num_batches: Optional[int] = 1):
        """
        Args:
            num_records (int): Total records to generate across all processes.
            num_process (int): Number of parallel processes to utilize.
            method (str): Transmission strategy - 'stream' (sequential) or 'batch' (bulk).
            port (int): Target API port.
            num_batches (int): Number of bulk groups (applicable only in 'batch' mode).
        """
        self.num_record = num_records
        self.num_process = num_process
        self.method = method
        self.port = port
        self.num_batches = num_batches
        
        # Endpoint definitions
        self.stream_url = f"http://localhost:{port}/receive-data"
        self.batch_url = f"http://localhost:{port}/receive-bulk-data"
        
        # Resolve data directory for fallback logging
        self.data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))

    def _create_record(self) -> dict:
        """
        Internal: Generates a single synthetic sensor observation.
        
        Returns:
            dict: Simulated sensor metrics (pH, Temperature, Humidity, etc.)
        """
        return { 
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "ph_level": round(random.uniform(5.5, 6.5), 2),
            "ec_tds": round(random.uniform(1.2, 2.5), 2),
            "water_temp": round(random.uniform(18.0, 22.0), 1),
            "air_temp": round(random.uniform(20.0, 28.0), 1),
            "humidity": random.randint(40, 70),
            "water_level": random.randint(70, 100)
        }

    def _save_to_disk(self, data):
        """
        Internal: Persists data to local storage when the primary endpoint is unreachable.
        Used as a high-reliability fallback mechanism.
        """
        os.makedirs(self.data_dir, exist_ok=True)
        filepath = os.path.join(self.data_dir, "failed_transmissions.json")
        
        log_entry = {
            "captured_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "method": self.method,
            "data": data
        }

        try:
            # Atomic line-append to JSONL file for multi-process safety and performance
            with open(filepath, 'a') as f:
                f.write(json.dumps(log_entry) + "\n")
            print(f"💾 Endpoint unreachable. Data cached to: failed_transmissions.json")
        except Exception as e:
            print(f"❌ Storage Failure: Could not persist fallback data: {e}")

    def _send_single(self, record: dict):
        """
        Internal: Performs a synchronous HTTP POST for a single record.
        Designed for execution within a thread pool.
        """
        try:
            response = requests.post(self.stream_url, json=record, timeout=5)
            response.raise_for_status()
        except Exception as e:
            print(f"⚠️ Stream Error: {e}")
            self._save_to_disk(record)

    def stream_data(self, count: int):
        """
        Transactional: Sends records sequentially via high-concurrency thread pool.
        
        Args:
            count (int): Number of records to stream for the current process.
        """
        pid = os.getpid()
        print(f"🚀 [Process {pid}] Streaming {count} sequential records...")
        
        records = [self._create_record() for _ in range(count)]
        
        # Parallelize IO operations to maximize CPU-time utilization during wait states
        with ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(self._send_single, records)

    def batch_data(self, count: int, num_batches: int = 1):
        """
        Transactional: Groups records and performs bulk HTTP POST transmissions.
        
        Args:
            count (int): Total records to transmit.
            num_batches (int): Partition count for batch grouping.
        """
        pid = os.getpid()
        print(f"🚀 [Process {pid}] Preparing {count} records in {num_batches} batch(es)...")
        
        batch_size = count // num_batches
        
        for i in range(num_batches):
            batch_records = [self._create_record() for _ in range(batch_size)]
            try:
                print(f"📦 [Process {pid}] Transmitting batch {i+1}/{num_batches}...")
                response = requests.post(self.batch_url, json=batch_records, timeout=10)
                response.raise_for_status()
            except Exception as e:
                print(f"⚠️ Batch Error: {e}")
                self._save_to_disk(batch_records)

    def start_generating(self):
        """
        Orchestration: Divides the total load across parallel OS processes.
        This is the main entry point for the generation task.
        """
        records_per_process = self.num_record // self.num_process
        processes = []

        print(f"🛠️  Initializing '{self.method.upper()}' engine with {self.num_process} worker(s)...")

        for _ in range(self.num_process):
            if self.method == "batch":
                p = multiprocessing.Process(
                    target=self.batch_data, 
                    args=(records_per_process, self.num_batches)
                )
            else:
                p = multiprocessing.Process(
                    target=self.stream_data, 
                    args=(records_per_process,)
                )
            
            p.start()
            processes.append(p)

        # Wait for all workers to finish execution
        for p in processes:
            p.join()
            
        print(f"✅ {self.method.upper()} extraction pipeline completed.")

if __name__ == "__main__":
    # --- DEFAULT CONFIGURATION FOR LOCAL TESTING ---
    # Total: 10 records, Processes: 2, Mode: Batch, Batches: 5
    generator = GenerateData(
        num_records=10, 
        num_process=2, 
        method="batch", 
        num_batches=5
    )
    
    generator.start_generating()
