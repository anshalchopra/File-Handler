import faker
import requests
import sqlite3
import os
from datetime import datetime

class DataGenerator:
    """Class to simulate high-load data production for stress testing."""

    def __init__(self, num_records: int = 100):
        self.num_records = num_records
        self.faker = faker.Faker()

    def generate_data(self):
        """Pre-generates a massive list of records in RAM to test OOM limits."""
        return [{
            "first_name": self.faker.first_name(),
            "last_name": self.faker.last_name(),
            "age": self.faker.random_int(min=18, max=99),
            "country": self.faker.country()
        } for _ in range(self.num_records)]

    def update_status(self, is_running, mode, progress, success=False):
        """Communicates progress and completion status to the Streamlit UI."""
        status_file = "/app/data/status.json"
        try:
            import json
            with open(status_file, "w") as f:
                json.dump({
                    "is_running": is_running, "mode": mode,
                    "progress": progress, "total": self.num_records,
                    "success": success
                }, f)
        except:
            pass

    def _log_completion(self, mode):
        """Persist successful run data to the SQLite Activity Log."""
        db_path = "/app/data/monitoring_history.db"
        if os.path.exists(db_path):
            try:
                conn = sqlite3.connect(db_path)
                conn.execute("INSERT INTO activity_logs (timestamp, mode, count) VALUES (?, ?, ?)",
                    (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), mode.upper(), self.num_records))
                conn.commit()
                conn.close()
            except Exception as e:
                print(f"Log Error: {e}")

    def send_data(self):
        """Tests Bulk POST limits by generating and sending 100% of data in one massive RAM payload."""
        url = "http://localhost:8000/receive-bulk-data"
        self.update_status(True, "bulk", 0)
        try:
            data = self.generate_data()
            self.update_status(True, "bulk", len(data) // 2)
            requests.post(url, json=data).raise_for_status()
            
            self._log_completion("bulk")
            self.update_status(False, "bulk", self.num_records, success=True)
        except Exception as e:
            print(f"Bulk Test Crash: {e}")
            self.update_status(False, "bulk", 0, success=False)

    def stream_data(self):
        """Tests Streaming POST limits by pre-loading RAM then iterating one-by-one."""
        url = "http://localhost:8000/receive-data"
        self.update_status(True, "stream", 0)
        try:
            data = self.generate_data()
            for i, record in enumerate(data):
                requests.post(url, json=record).raise_for_status()
                if i % 1000 == 0:
                    self.update_status(True, "stream", i)
            
            self._log_completion("stream")
            self.update_status(False, "stream", self.num_records, success=True)
        except Exception as e:
            print(f"Stream Test Crash: {e}")
            self.update_status(False, "stream", 0, success=False)

    


