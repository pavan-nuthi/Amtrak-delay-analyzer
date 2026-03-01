"""
Amtrak Delay Data Collector - Standalone Scraper
=================================================
Run this script periodically (e.g., every 15 minutes via cron) to accumulate
real-time Amtrak delay data into an SQLite database.

Usage:
    python amtrak_scraper.py

Cron example (every 15 minutes):
    */15 * * * * cd /path/to/project && python amtrak_scraper.py >> scraper.log 2>&1
"""

import requests
import sqlite3
import time
import json
from datetime import datetime, timezone

import os

# ── Configuration ──────────────────────────────────────────────────────────
# use absolute path so cron always writes to the project directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, "amtrak_delays.db")
API_URL = "https://api-v3.amtraker.com/v3/trains"
HEADERS = {
    "User-Agent": "STA220-AmtrakProject/1.0 (UC Davis Student Project)"
}


def create_tables(conn):
    """Create the database schema if tables don't already exist."""
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS routes (
            route_name TEXT PRIMARY KEY,
            sample_origin TEXT,
            sample_destination TEXT,
            first_seen TEXT,
            last_seen TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stations (
            station_code TEXT PRIMARY KEY,
            station_name TEXT,
            timezone TEXT,
            is_bus_stop INTEGER DEFAULT 0
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS delay_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scrape_time TEXT NOT NULL,
            train_number TEXT NOT NULL,
            train_id TEXT NOT NULL,
            route_name TEXT,
            station_code TEXT,
            station_name TEXT,
            scheduled_arrival TEXT,
            actual_arrival TEXT,
            scheduled_departure TEXT,
            actual_departure TEXT,
            delay_arrival_min REAL,
            delay_departure_min REAL,
            status TEXT,
            latitude REAL,
            longitude REAL,
            velocity REAL,
            train_state TEXT,
            FOREIGN KEY (route_name) REFERENCES routes(route_name),
            FOREIGN KEY (station_code) REFERENCES stations(station_code)
        )
    """)

    conn.commit()


def parse_delay_minutes(scheduled, actual):
    """Calculate delay in minutes between scheduled and actual times."""
    if not scheduled or not actual:
        return None
    try:
        sch = datetime.fromisoformat(scheduled)
        act = datetime.fromisoformat(actual)
        delta = (act - sch).total_seconds() / 60.0
        return round(delta, 1)
    except (ValueError, TypeError):
        return None


def fetch_and_store(conn):
    """Fetch current train data from Amtraker API and store in database."""
    scrape_time = datetime.now(timezone.utc).isoformat()

    # fetch data from API
    response = requests.get(API_URL, headers=HEADERS, timeout=30)
    response.raise_for_status()
    data = response.json()

    cursor = conn.cursor()
    trains_processed = 0
    stations_logged = 0

    for train_num, train_list in data.items():
        for train in train_list:
            route_name = train.get("routeName", "Unknown")
            train_number = train.get("trainNum", train_num)
            train_id = train.get("trainID", "")
            lat = train.get("lat")
            lon = train.get("lon")
            velocity = train.get("velocity")
            train_state = train.get("trainState", "")
            orig = train.get("origName", "")
            dest = train.get("destName", "")

            # upsert route
            cursor.execute("""
                INSERT INTO routes (route_name, sample_origin, sample_destination, first_seen, last_seen)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(route_name) DO UPDATE SET
                    last_seen = excluded.last_seen
            """, (route_name, orig, dest, scrape_time, scrape_time))

            # process each station stop
            for station in train.get("stations", []):
                code = station.get("code", "")
                name = station.get("name", "")
                tz = station.get("tz", "")
                bus = 1 if station.get("bus", False) else 0

                # upsert station
                cursor.execute("""
                    INSERT INTO stations (station_code, station_name, timezone, is_bus_stop)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(station_code) DO UPDATE SET
                        station_name = excluded.station_name
                """, (code, name, tz, bus))

                # only log stations that have actual arrival/departure data
                sch_arr = station.get("schArr", "")
                act_arr = station.get("arr", "")
                sch_dep = station.get("schDep", "")
                act_dep = station.get("dep", "")
                status = station.get("status", "")

                delay_arr = parse_delay_minutes(sch_arr, act_arr)
                delay_dep = parse_delay_minutes(sch_dep, act_dep)

                cursor.execute("""
                    INSERT INTO delay_logs (
                        scrape_time, train_number, train_id, route_name,
                        station_code, station_name,
                        scheduled_arrival, actual_arrival,
                        scheduled_departure, actual_departure,
                        delay_arrival_min, delay_departure_min,
                        status, latitude, longitude, velocity, train_state
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    scrape_time, train_number, train_id, route_name,
                    code, name,
                    sch_arr, act_arr, sch_dep, act_dep,
                    delay_arr, delay_dep,
                    status, lat, lon, velocity, train_state
                ))
                stations_logged += 1

            trains_processed += 1

    conn.commit()
    return trains_processed, stations_logged


def main():
    """Main entry point for the scraper."""
    print(f"[{datetime.now().isoformat()}] Starting Amtrak data collection...")

    conn = sqlite3.connect(DB_PATH)
    create_tables(conn)

    try:
        trains, stations = fetch_and_store(conn)
        print(f"  ✓ Collected data for {trains} trains, {stations} station stops")
    except requests.RequestException as e:
        print(f"  ✗ API request failed: {e}")
    except Exception as e:
        print(f"  ✗ Error: {e}")
    finally:
        conn.close()

    print(f"[{datetime.now().isoformat()}] Done.\n")


if __name__ == "__main__":
    main()
