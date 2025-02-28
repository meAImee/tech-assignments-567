import os
import csv
import mysql.connector

# Load environment variables
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

def get_db_connection():
    """Establish a connection to the MySQL database."""
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )

def seed_database():
    """Create tables and seed data from CSV files."""
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
                timestamp DATETIME NOT NULL,
                unit VARCHAR(10) NOT NULL,
                value FLOAT NOT NULL
            )
        """)

        with open(file_path, newline='') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)  # Skip header row
            data = [(float(row[0]), row[1], row[2]) for row in reader]
            cursor.executemany(f"INSERT INTO {table} (timestamp, unit, value) VALUES (%s, %s, %s)", data)

    conn.commit()
    cursor.close()
    conn.close()
