import pytest
from unittest.mock import MagicMock, patch, mock_open
import sqlite3
from src.updateDB import create_tables, insert_city, insert_weather, write_weather_data_to_db

CSV_CONTENT = """city,weather,temp,pressure,humidity,temp_min,temp_max
Mumbai,Clear sky,300,1013,70,298,302
Delhi,Partly cloudy,305,1010,65,303,307
"""

@pytest.fixture
def db_connection():
    conn=sqlite3.connect(':memory:')
    yield conn
    conn.close()

def test_create_tables_success(db_connection):
    create_tables(db_connection)
    cursor=db_connection.cursor()

    tables = ['City', 'Weather']

    for table in tables:
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'")
        assert cursor.fetchone() is not None, f"{table} table was not created"

def test_insert_city(db_connection):
    create_tables(db_connection) 
    
    city_id = insert_city(db_connection, "Mumbai")
    assert isinstance(city_id, int) and city_id > 0, "Should insert new city and return a valid city_id"

    city_id_again = insert_city(db_connection, "Mumbai")
    assert city_id == city_id_again, "Should not insert duplicate city, should return the same city_id"

    cursor = db_connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM City WHERE name = ?", ("Mumbai",))
    count = cursor.fetchone()[0]
    assert count == 1, "There should be only one Mumbai in the table"

def test_insert_weather(db_connection):
    create_tables(db_connection)
    cursor = db_connection.cursor()

    city_id = insert_city(db_connection, "Mumbai")
    weather_data = {
        "weather": "Clear sky",
        "temp": 300.0,
        "pressure": 1013.0,
        "humidity": 70.0,
        "temp_min": 298.0,
        "temp_max": 302.0
    }
    insert_weather(db_connection, city_id, weather_data)

    cursor.execute("SELECT city_id, weather, temp, pressure, humidity, temp_min, temp_max FROM Weather WHERE city_id = ?",(city_id,))
    result = cursor.fetchone()

    assert result is not None, "Weather data was not inserted"
    assert result[0] == city_id, "City id mismatch"
