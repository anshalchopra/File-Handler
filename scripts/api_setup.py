import fastapi
from pydantic import BaseModel
import pandas as pd
import os
from datetime import datetime

app = fastapi.FastAPI()

# Define the structure of your data
class UserData(BaseModel):
    first_name: str
    last_name: str
    age: int
    country: str

DATA_PATH = "/app/data/sandbox_data.csv"

from typing import List

@app.post("/receive-data")
def receive_data(user: UserData):
    # Convert incoming data to a dictionary
    new_entry = user.model_dump()
    new_entry['received_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Save to CSV (Append mode)
    df = pd.DataFrame([new_entry])
    file_exists = os.path.isfile(DATA_PATH)
    
    df.to_csv(DATA_PATH, mode='a', index=False, header=not file_exists)
    
    return {"status": "success", "message": f"Data for {user.first_name} stored successfully!"}

@app.post("/receive-bulk-data")
def receive_bulk_data(users: List[UserData]):
    print(f"--- Bulk Upload Started: Processing {len(users)} records ---")
    data = [u.model_dump() for u in users]
    df = pd.DataFrame(data)
    
    # CRASH ATTEMPT: We will hold multiple massive copies in a list
    # and perform heavy sorting/manipulation on each
    waste_of_ram = []
    
    for i in range(12): # Increased iterations
        print(f"Creating RAM-heavy copy #{i+1}...")
        temp_df = df.copy(deep=True) # Force complete memory allocation
        # Add high-precision columns to increase size
        temp_df['noise_1'] = [0.123456789] * len(temp_df)
        temp_df['noise_2'] = [0.987654321] * len(temp_df)
        temp_df['id'] = range(len(temp_df))
        
        # Sort by something slow
        temp_df = temp_df.sort_values(by=['country', 'age', 'first_name'])
        
        # Append to the list to KEEP it in memory (preventing Garbage Collection)
        waste_of_ram.append(temp_df)
    
    # Finally write the original
    df.to_csv(DATA_PATH, mode='a', index=False, header=not os.path.isfile(DATA_PATH))
    print(f"--- Bulk Upload Finished ---")
    return {"status": "success", "info": f"Processed {len(users)} rows using {len(waste_of_ram)} full copies."}


if __name__ == "__main__":
    import uvicorn
    # app_dir="/app/scripts" tells uvicorn where to find your file
    uvicorn.run("api_setup:app", host="0.0.0.0", port=8000, reload=True, app_dir="/app/scripts")
