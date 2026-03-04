import os
from datetime import datetime
from typing import List
import fastapi
from pydantic import BaseModel
import pandas as pd
import uvicorn

app = fastapi.FastAPI()
DATA_PATH = "/app/data/sandbox_data.csv"

class UserData(BaseModel):
    first_name: str
    last_name: str
    age: int
    country: str

@app.post("/receive-data")
def receive_data(user: UserData):
    """Processes a single record and appends to the sandbox CSV."""
    new_entry = user.model_dump()
    new_entry['received_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    df = pd.DataFrame([new_entry])
    df.to_csv(DATA_PATH, mode='a', index=False, header=not os.path.exists(DATA_PATH))
    return {"status": "success", "message": f"Processed {user.first_name}"}

@app.post("/receive-bulk-data")
def receive_bulk_data(users: List[UserData]):
    """Processes a large batch of records in a single RAM-intensive operation."""
    df = pd.DataFrame([u.model_dump() for u in users])
    df.to_csv(DATA_PATH, mode='a', index=False, header=not os.path.exists(DATA_PATH))
    return {"status": "success", "rows": len(users)}

if __name__ == "__main__":
    uvicorn.run("api_setup:app", host="0.0.0.0", port=8000, reload=True, app_dir="/app/scripts")
