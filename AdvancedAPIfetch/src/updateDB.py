import csv
import sqlite3
import os

CSV_FILENAME = "weather.csv"
DB_FILENAME = "weather_data.db"

def create_tables(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS City (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city_id INTEGER,
            weather TEXT,
            temp REAL,
            pressure REAL,
            humidity REAL,
            temp_min REAL,
            temp_max REAL,
            date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (city_id) REFERENCES City(id)
        )
        """)

        conn.commit()
    except sqlite3.DatabaseError as e:
        print(f"Database error while creating tables: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error while creating tables: {e}")
        raise

def insert_city(conn, city_name):
    try:
        cursor = conn.cursor()

        cursor.execute("SELECT id FROM City WHERE name = ?", (city_name,))
        city_id = cursor.fetchone()

        if not city_id:
            cursor.execute("INSERT INTO City (name) VALUES (?)", (city_name,))
            city_id = cursor.lastrowid
        else:
            city_id = city_id[0]

        conn.commit()

        return city_id
    except sqlite3.IntegrityError as e:
        print(f"Integrity error when inserting city: {e}")
        raise
    except sqlite3.DatabaseError as e:
        print(f"Database error while inserting city: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error while inserting city: {e}")
        raise

def insert_weather(conn, city_id, weather_data):
    try:
        cursor = conn.cursor()

        cursor.execute("""
        INSERT INTO Weather (city_id, weather, temp, pressure, humidity, temp_min, temp_max)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            city_id,
            weather_data["weather"],
            weather_data["temp"],
            weather_data["pressure"],
            weather_data["humidity"],
            weather_data["temp_min"],
            weather_data["temp_max"]
        ))

        conn.commit()
    except sqlite3.IntegrityError as e:
        print(f"Integrity error while inserting weather data: {e}")
        raise
    except sqlite3.DatabaseError as e:
        print(f"Database error while inserting weather data: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error while inserting weather data: {e}")
        raise

def write_weather_data_to_db(conn):
    try:
        if not os.path.exists(CSV_FILENAME):
            print(f"{CSV_FILENAME} not found!")
            return

        with open(CSV_FILENAME, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)

            for row in reader:
                try:
                    city_name = row["city"]

                    city_id = insert_city(conn, city_name)

                    weather_data = {
                        "weather": row["weather"],
                        "temp": float(row["temp"]),
                        "pressure": float(row["pressure"]),
                        "humidity": float(row["humidity"]),
                        "temp_min": float(row["temp_min"]),
                        "temp_max": float(row["temp_max"])
                    }

                    insert_weather(conn, city_id, weather_data)

                except KeyError as e:
                    print(f"Missing expected column in CSV: {e}")
                    continue
                except ValueError as e:
                    print(f"Invalid value encountered while processing row: {e}")
                    continue
                except Exception as e:
                    print(f"Unexpected error while processing row: {e}")
                    continue

        print("Weather data successfully updated from CSV into SQLite.")

    except FileNotFoundError as e:
        print(f"CSV file not found: {e}")
    except sqlite3.DatabaseError as e:
        print(f"Database error while updating data: {e}")
    except Exception as e:
        print(f"Unexpected error while updating data: {e}")


def update_db_from_csv():
    try:
        conn = sqlite3.connect(DB_FILENAME)
        create_tables(conn)
        write_weather_data_to_db(conn)
        conn.close()
    except sqlite3.DatabaseError as e:
        print(f"Database error while updating data: {e}")
    except Exception as e:
        print(f"Unexpected error while updating data: {e}")
        
if __name__ == "__main__":
    update_db_from_csv()
