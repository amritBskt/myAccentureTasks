import sqlite3
import pytest
from src.readDB import read_all_data_from_db, DB_FILENAME
import src.readDB as readDB

@pytest.fixture
def setup_in_memory_db(monkeypatch):
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE City (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL
    )
    """)
    
    cursor.execute("""
    CREATE TABLE Weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city_id INTEGER,
        weather TEXT,
        temp REAL,
        pressure INTEGER,
        humidity INTEGER,
        temp_min REAL,
        temp_max REAL,
        date TEXT DEFAULT CURRENT_DATE,
        FOREIGN KEY(city_id) REFERENCES City(id)
    )
    """)

    cursor.execute("INSERT INTO City (name) VALUES (?)", ("Mumbai",))
    cursor.execute("INSERT INTO City (name) VALUES (?)", ("Delhi",))

    cursor.execute("SELECT id FROM City WHERE name = 'Mumbai'")
    mumbai_id = cursor.fetchone()[0]

    cursor.execute("SELECT id FROM City WHERE name = 'Delhi'")
    delhi_id = cursor.fetchone()[0]

    cursor.execute("""
        INSERT INTO Weather (city_id, weather, temp, pressure, humidity, temp_min, temp_max)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (mumbai_id, "Clear sky", 300, 1013, 70, 298, 302))

    cursor.execute("""
        INSERT INTO Weather (city_id, weather, temp, pressure, humidity, temp_min, temp_max)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (delhi_id, "Partly cloudy", 305, 1010, 65, 303, 307))

    conn.commit()

    def connect_override(_filename):
        return conn

    monkeypatch.setattr(readDB.sqlite3, "connect", connect_override)

    yield conn

    conn.close()

def test_read_all_data_from_db_output(setup_in_memory_db, capsys):
    read_all_data_from_db()

    captured = capsys.readouterr()
    output = captured.out

    assert "Cities in the database:" in output
    assert "Mumbai" in output
    assert "Delhi" in output
    assert "Weather data in the database:" in output
    assert "Clear sky" in output
    assert "Partly cloudy" in output
    assert "Temperature: 300" in output
    assert "Humidity: 70" in output
    assert "Temperature: 305" in output
    assert "Humidity: 65" in output

def test_database_error_handling(monkeypatch, capsys):
    def raise_db_error(_):
        raise sqlite3.DatabaseError("Test DB error")

    monkeypatch.setattr(readDB.sqlite3, "connect", raise_db_error)

    read_all_data_from_db()
    captured = capsys.readouterr()
    assert "Database error while reading data: Test DB error" in captured.out


def test_unexpected_exception_handling(monkeypatch, capsys):
    def raise_general_error(_):
        raise Exception("Unexpected test error")

    monkeypatch.setattr(readDB.sqlite3, "connect", raise_general_error)

    read_all_data_from_db()
    captured = capsys.readouterr()
    assert "Unexpected error while reading data: Unexpected test error" in captured.out

