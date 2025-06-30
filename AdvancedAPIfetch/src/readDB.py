import sqlite3

DB_FILENAME = "weather_data.db"

def read_all_data_from_db():
    try:
        conn = sqlite3.connect(DB_FILENAME)
        cursor = conn.cursor()

        cursor.execute("SELECT id, name FROM City")
        cities = cursor.fetchall()
        print("Cities in the database:")
        for city in cities:
            print(f"ID: {city[0]}, Name: {city[1]}")
        
        print("\nWeather data in the database:")

        cursor.execute("""
        SELECT w.id, c.name AS city_name, w.weather, w.temp, w.pressure, w.humidity, 
               w.temp_min, w.temp_max, w.date
        FROM Weather w
        JOIN City c ON w.city_id = c.id
        """)
        weather_data = cursor.fetchall()
        
        for weather in weather_data:
            print(f"Weather ID: {weather[0]}")
            print(f"City: {weather[1]}")
            print(f"Weather: {weather[2]}")
            print(f"Temperature: {weather[3]}")
            print(f"Pressure: {weather[4]}")
            print(f"Humidity: {weather[5]}")
            print(f"Min Temp: {weather[6]}")
            print(f"Max Temp: {weather[7]}")
            print(f"Date: {weather[8]}")
            print("-" * 40)

        conn.close()

    except sqlite3.DatabaseError as e:
        print(f"Database error while reading data: {e}")
    except Exception as e:
        print(f"Unexpected error while reading data: {e}")

if __name__ == "__main__":
    read_all_data_from_db()
