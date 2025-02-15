import os
import csv
import mysql.connector
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

# Load environment variables (unchanged)
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

# Database connection and seeding functions (unchanged)
def get_db_connection():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )

def seed_database():
    conn = get_db_connection()
    cursor = conn.cursor()

    tables = {
        "temperature": "./sample/temperature.csv",
        "humidity": "./sample/humidity.csv",
        "light": "./sample/light.csv"
    }

    for table, file_path in tables.items():
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                value FLOAT NOT NULL,
                unit VARCHAR(10) NOT NULL,
                timestamp DATETIME NOT NULL
            )
        """)

        with open(file_path, newline='') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)  # Skip header
            data = [(float(row[1]), row[2], row[0]) for row in reader]
            cursor.executemany(f"INSERT INTO {table} (value, unit, timestamp) VALUES (%s, %s, %s)", data)

    conn.commit()
    cursor.close()
    conn.close()

# FastAPI app setup (unchanged)
app = FastAPI()

@app.on_event("startup")
def startup_event():
    seed_database()

@app.get("/", response_class=HTMLResponse)
def read_root(request: Request):
    return "Hello, World!"

# Pydantic model (unchanged)
class SensorData(BaseModel):
    value: float
    unit: str
    timestamp: Optional[str] = Field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# Reordered routes: count endpoint comes first
@app.get("/api/{sensor_type}/count")
def get_count(sensor_type: str):
    if sensor_type not in ["temperature", "humidity", "light"]:
        raise HTTPException(status_code=404, detail="Invalid sensor type")  # Changed from 400 to 404
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {sensor_type}")
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return {"count": count}

# Original routes adjusted for order
@app.get("/api/{sensor_type}")
def get_all_data(sensor_type: str, order_by: Optional[str] = Query(None, alias="order-by"),
                 start_date: Optional[str] = None, end_date: Optional[str] = None):
    if sensor_type not in ["temperature", "humidity", "light"]:
        raise HTTPException(status_code=404, detail="Invalid sensor type")

    query = f"SELECT * FROM {sensor_type}"
    filters = []
    if start_date:
        filters.append(f"timestamp >= STR_TO_DATE('{start_date}', '%Y-%m-%d %H:%i:%s')")
    if end_date:
        filters.append(f"timestamp <= STR_TO_DATE('{end_date}', '%Y-%m-%d %H:%i:%s')")
    
    if filters:
        query += " WHERE " + " AND ".join(filters)
    
    if order_by in ["value", "timestamp"]:
        query += f" ORDER BY {order_by}"

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    
    return data

# Remaining routes (unchanged and truncated for brevity)
@app.post("/api/{sensor_type}")
def insert_data(sensor_type: str, data: SensorData):
    # ... (existing code)

@app.get("/api/{sensor_type}/{id}")
def get_data_by_id(sensor_type: str, id: int):
    # ... (existing code)

@app.put("/api/{sensor_type}/{id}")
def update_data(sensor_type: str, id: int, data: SensorData):
    # ... (existing code)

@app.delete("/api/{sensor_type}/{id}")
def delete_data(sensor_type: str, id: int):
    # ... (existing code)

# Server setup (unchanged)
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app="app.main:app", host="0.0.0.0", port=6543, reload=True)