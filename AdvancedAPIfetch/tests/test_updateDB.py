import pytest
from unittest.mock import mock_open, MagicMock
import sqlite3
from src.updateDB import create_tables, insert_city, insert_weather, write_weather_data_to_db, update_db_from_csv
import src.updateDB as updateDB

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

def test_write_weather_data_to_db_file_not_found(monkeypatch, db_connection, capsys):
    monkeypatch.setattr(updateDB.os.path, "exists", lambda _: False)
    write_weather_data_to_db(db_connection)

    captured = capsys.readouterr()
    assert "weather.csv not found!" in captured.out


def test_write_weather_data_to_db_keyerror(monkeypatch, db_connection, capsys):
    create_tables(db_connection)
    monkeypatch.setattr(updateDB.os.path, "exists", lambda _: True)

    broken_csv = """city,temp,pressure,humidity,temp_min,temp_max\nMumbai,300,1013,70,298,302"""
    mock_open_obj = mock_open(read_data=broken_csv)
    monkeypatch.setattr("builtins.open", mock_open_obj)

    write_weather_data_to_db(db_connection)
    captured = capsys.readouterr()
    assert "Missing expected column in CSV" in captured.out


def test_write_weather_data_to_db_valueerror(monkeypatch, db_connection, capsys):
    create_tables(db_connection)
    monkeypatch.setattr(updateDB.os.path, "exists", lambda _: True)

    invalid_csv = """city,weather,temp,pressure,humidity,temp_min,temp_max\nMumbai,Clear sky,not_a_float,1013,70,298,302"""
    mock_open_obj = mock_open(read_data=invalid_csv)
    monkeypatch.setattr("builtins.open", mock_open_obj)


    write_weather_data_to_db(db_connection)
    captured = capsys.readouterr()
    assert "Invalid value encountered while processing row" in captured.out


def test_write_weather_data_to_db_general_exception(monkeypatch, db_connection, capsys):
    monkeypatch.setattr(updateDB.os.path, "exists", lambda _: True)

    valid_csv = """city,weather,temp,pressure,humidity,temp_min,temp_max\nMumbai,Clear sky,300,1013,70,298,302"""
    mock_open_obj = mock_open(read_data=valid_csv)
    monkeypatch.setattr("builtins.open", mock_open_obj)


    def failing_insert_city(*args, **kwargs):
        raise Exception("Simulated error")

    monkeypatch.setattr(updateDB, "insert_city", failing_insert_city)

    write_weather_data_to_db(db_connection)
    captured = capsys.readouterr()
    assert "Unexpected error while processing row" in captured.out


def test_update_db_from_csv_database_error(monkeypatch, capsys):
    def fake_connect(*args, **kwargs):
        raise sqlite3.DatabaseError("Simulated DB failure")

    monkeypatch.setattr(updateDB.sqlite3, "connect", fake_connect)

    update_db_from_csv()
    captured = capsys.readouterr()
    assert "Database error while updating data: Simulated DB failure" in captured.out


def test_update_db_from_csv_general_exception(monkeypatch, capsys):
    def fake_connect(*args, **kwargs):
        raise Exception("Unexpected failure")

    monkeypatch.setattr(updateDB.sqlite3, "connect", fake_connect)

    update_db_from_csv()
    captured = capsys.readouterr()
    assert "Unexpected error while updating data: Unexpected failure" in captured.out

def test_insert_weather_general_exception(monkeypatch):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.execute.side_effect = Exception("Unexpected error")

    weather_data = {
        "weather": "Clear",
        "temp": 300,
        "pressure": 1013,
        "humidity": 70,
        "temp_min": 298,
        "temp_max": 302
    }

    with pytest.raises(Exception, match="Unexpected error"):
        insert_weather(mock_conn, 1, weather_data)


from unittest.mock import MagicMock

def test_insert_city_integrity_error(monkeypatch):
    conn = MagicMock()
    mock_cursor = MagicMock()
    conn.cursor.return_value = mock_cursor
    mock_cursor.execute.side_effect = sqlite3.IntegrityError("Mock IntegrityError")

    with pytest.raises(sqlite3.IntegrityError):
        insert_city(conn, "Mumbai")


def test_insert_city_database_error(monkeypatch):
    conn = MagicMock()
    mock_cursor = MagicMock()
    conn.cursor.return_value = mock_cursor
    mock_cursor.execute.side_effect = sqlite3.DatabaseError("Mock DBError")

    with pytest.raises(sqlite3.DatabaseError):
        insert_city(conn, "Mumbai")


def test_insert_city_unexpected_exception(monkeypatch):
    conn = MagicMock()
    mock_cursor = MagicMock()
    conn.cursor.return_value = mock_cursor
    mock_cursor.execute.side_effect = Exception("Unexpected failure")

    with pytest.raises(Exception, match="Unexpected failure"):
        insert_city(conn, "Mumbai")

def test_insert_weather_integrity_error(monkeypatch):
    conn = MagicMock()
    mock_cursor = MagicMock()
    conn.cursor.return_value = mock_cursor
    mock_cursor.execute.side_effect = sqlite3.IntegrityError("Mock IntegrityError")

    weather_data = {
        "weather": "Clear sky",
        "temp": 300,
        "pressure": 1013,
        "humidity": 70,
        "temp_min": 298,
        "temp_max": 302
    }

    with pytest.raises(sqlite3.IntegrityError):
        insert_weather(conn, 1, weather_data)


def test_insert_weather_database_error(monkeypatch):
    conn = MagicMock()
    mock_cursor = MagicMock()
    conn.cursor.return_value = mock_cursor
    mock_cursor.execute.side_effect = sqlite3.DatabaseError("Mock DBError")

    weather_data = {
        "weather": "Clear sky",
        "temp": 300,
        "pressure": 1013,
        "humidity": 70,
        "temp_min": 298,
        "temp_max": 302
    }

    with pytest.raises(sqlite3.DatabaseError):
        insert_weather(conn, 1, weather_data)
