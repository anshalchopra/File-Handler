import os
import sqlite3
import json
import logging
from datetime import datetime
import faker
import requests

# --- 1. CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("SyncGenerator")

class SyncGenerateData:
    def __init__(self, num_records, method, port=8000, num_batches=1):
        self.num_record = num_records
        self.method = method
        self.port = port
        self.num_batches = num_batches
        self.fake = faker.Faker()
        self.stream_url = f"http://localhost:{port}/receive-data"
        self.batch_url = f"http://localhost:{port}/receive-bulk-data"
        
    def _create_record(self):
        return {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "ph_level": round(self.fake.random.uniform(6.0, 9.0), 2),
            "ec_tds": round(self.fake.random.uniform(200, 800), 2),
            "water_temp": round(self.fake.random.uniform(15.0, 30.0), 2),
            "air_temp": round(self.fake.random.uniform(18.0, 35.0), 2),
            "humidity": self.fake.random.randint(40, 80),
            "water_level": self.fake.random.randint(0, 100)
        }

    def stream_data(self, count):
        """Sequential streaming: Silent for performance."""
        for _ in range(count):
            try:
                record = self._create_record()
                resp = requests.post(self.stream_url, json=record, timeout=30)
                resp.raise_for_status()
            except Exception as e:
                logger.error(f"⚠️ Stream Error: {e}")
                break
        print(f"✨ Stream Finished: {count} records sent.")

    def batch_data(self, total_count, num_batches):
        """Sequential batching."""
        recs_per_batch = total_count // num_batches
        for i in range(num_batches):
            batch = [self._create_record() for _ in range(recs_per_batch)]
            try:
                resp = requests.post(self.batch_url, json=batch, timeout=30)
                resp.raise_for_status()
            except Exception as e:
                logger.error(f"⚠️ Batch Error at {i+1}: {e}")
                break
        print(f"✨ Batch Finished: {total_count} records sent.")

    def start_generating(self):
        if self.method == "batch":
            self.batch_data(self.num_record, self.num_batches)
        else:
            self.stream_data(self.num_record)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--records", type=int, default=500)
    parser.add_argument("--method", type=str, default="stream")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--batches", type=int, default=1)
    args = parser.parse_args()
    
    gen = SyncGenerateData(args.records, args.method, args.port, args.batches)
    gen.start_generating()
