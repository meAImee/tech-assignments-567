import os
import csv
import mysql.connector
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

# Load environment variables
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

# Establish database connection
def get_db_connection():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )

# Create tables and seed data
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
            next(reader)  # Skip header row
            data = [(float(row[1]), row[2], row[0]) for row in reader]  # Fix column order
            cursor.executemany(f"INSERT INTO {table} (value, unit, timestamp) VALUES (%s, %s, %s)", data)

    conn.commit()
    cursor.close()
    conn.close()

# Define FastAPI app
app = FastAPI()

# Run database seeding on startup
@app.on_event("startup")
def startup_event():
    seed_database()

@app.get("/", response_class=HTMLResponse)
def read_root(request: Request):
    return "Hello, World!"  # Placeholder response

# Pydantic model for request body
class SensorData(BaseModel):
    value: float
    unit: str
    timestamp: Optional[str] = Field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# Routes for API
@app.get("/api/{sensor_type}")
def get_all_data(sensor_type: str, order_by: Optional[str] = Query(None, alias="order-by"),
                 start_date: Optional[str] = None, end_date: Optional[str] = None):
    if sensor_type not in ["temperature", "humidity", "light"]:
        raise HTTPException(status_code=404, detail="Invalid sensor type")

    # Base query
    query = f"SELECT * FROM {sensor_type}"
    
    # Add date filters if provided
    filters = []
    if start_date:
        filters.append(f"timestamp >= '{start_date}'")
    if end_date:
        filters.append(f"timestamp <= '{end_date}'")
    
    if filters:
        query += " WHERE " + " AND ".join(filters)
    
    # Add ordering if provided
    if order_by in ["value", "timestamp"]:
        query += f" ORDER BY {order_by}"

    # Execute the query
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    
    return data

@app.post("/api/{sensor_type}")
def insert_data(sensor_type: str, data: SensorData):
    if sensor_type not in ["temperature", "humidity", "light"]:
        raise HTTPException(status_code=404, detail="Invalid sensor type")

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"INSERT INTO {sensor_type} (value, unit, timestamp) VALUES (%s, %s, %s)",
                   (data.value, data.unit, data.timestamp))
    conn.commit()
    last_id = cursor.lastrowid
    cursor.close()
    conn.close()
    return {"id": last_id}

@app.get("/api/{sensor_type}/{id}")
def get_data_by_id(sensor_type: str, id: int):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(f"SELECT * FROM {sensor_type} WHERE id = %s", (id,))
    data = cursor.fetchone()
    cursor.close()
    conn.close()
    if not data:
        raise HTTPException(status_code=404, detail="ID not found")
    return data

@app.put("/api/{sensor_type}/{id}")
def update_data(sensor_type: str, id: int, data: SensorData):
    if sensor_type not in ["temperature", "humidity", "light"]:
        raise HTTPException(status_code=404, detail="Invalid sensor type")

    conn = get_db_connection()
    cursor = conn.cursor()

    # Check if the record exists
    cursor.execute(f"SELECT * FROM {sensor_type} WHERE id = %s", (id,))
    existing_data = cursor.fetchone()

    if not existing_data:
        cursor.close()
        conn.close()
        raise HTTPException(status_code=404, detail="ID not found")

    # Update the record
    cursor.execute(
        f"UPDATE {sensor_type} SET value = %s, unit = %s, timestamp = %s WHERE id = %s",
        (data.value, data.unit, data.timestamp, id)
    )
    conn.commit()

    cursor.close()
    conn.close()

    return {"status": "success", "message": "Record updated successfully"}

@app.get("/api/{sensor_type}/count")
def get_count(sensor_type: str):
    if sensor_type not in ["temperature", "humidity", "light"]:
        raise HTTPException(status_code=400, detail="Invalid sensor type")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {sensor_type}")
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return {"count": count}

@app.delete("/api/{sensor_type}/{id}")
def delete_data(sensor_type: str, id: int):
    if sensor_type not in ["temperature", "humidity", "light"]:
        raise HTTPException(status_code=404, detail="Invalid sensor type")

    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Check if the record exists
    cursor.execute(f"SELECT * FROM {sensor_type} WHERE id = %s", (id,))
    data = cursor.fetchone()
    
    if not data:
        cursor.close()
        conn.close()
        raise HTTPException(status_code=404, detail="ID not found")
    
    # Delete the record
    cursor.execute(f"DELETE FROM {sensor_type} WHERE id = %s", (id,))
    conn.commit()
    
    cursor.close()
    conn.close()
    
    return {"status": "success", "message": "Record deleted successfully"}

# Run FastAPI server
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app="app.main:app", host="0.0.0.0", port=6543, reload=True)