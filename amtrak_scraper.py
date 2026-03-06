"""
Amtrak Delay Data Collector + ML Prediction Pipeline
=====================================================
Run this script periodically (e.g., every 15 minutes via cron) to:
1. Scrape real-time Amtrak train data from the Amtraker API
2. Store delay records in SQLite
3. Run ML predictions on new data (if a trained model exists)

Usage:
    python amtrak_scraper.py

Cron example (every 15 minutes):
    */15 * * * * /path/to/.venv/bin/python /path/to/amtrak_scraper.py >> /path/to/scraper.log 2>&1
"""

import requests
import sqlite3
import time
import json
import os
import pickle
import numpy as np
from datetime import datetime, timezone

# ── Configuration ──────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, "amtrak_delays.db")
MODEL_PATH = os.path.join(SCRIPT_DIR, "delay_model.pkl")
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
            origin TEXT,
            destination TEXT,
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

    # predictions table — stores ML model predictions alongside actuals
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scrape_time TEXT NOT NULL,
            train_number TEXT,
            route_name TEXT,
            station_code TEXT,
            station_name TEXT,
            predicted_delay_min REAL,
            actual_delay_min REAL,
            prediction_error REAL,
            model_version TEXT
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


def load_model():
    """Load the trained ML model and its encoders if they exist."""
    if not os.path.exists(MODEL_PATH):
        return None
    try:
        with open(MODEL_PATH, "rb") as f:
            model_data = pickle.load(f)
        print(f"  ✓ Loaded ML model (trained: {model_data.get('trained_at', 'unknown')})")
        return model_data
    except Exception as e:
        print(f"  ⚠ Could not load model: {e}")
        return None


def predict_delays(model_data, records, scrape_time):
    """Run ML predictions on the scraped records."""
    model = model_data["model"]
    route_encoder = model_data["route_encoder"]
    station_encoder = model_data["station_encoder"]
    feature_cols = model_data["feature_cols"]
    median_velocity = model_data.get("median_velocity", 0)

    predictions = []

    for rec in records:
        try:
            # extract features matching the training pipeline
            sch_arr = rec.get("scheduled_arrival", "")
            if not sch_arr:
                continue

            sch_dt = datetime.fromisoformat(sch_arr)
            hour = sch_dt.hour
            day_of_week = sch_dt.weekday()
            is_weekend = 1 if day_of_week >= 5 else 0

            # encode route — use -1 for unseen routes
            route = rec.get("route_name", "Unknown")
            if route in route_encoder:
                route_encoded = route_encoder[route]
            else:
                continue  # skip unseen routes

            # encode station — use -1 for unseen stations
            station = rec.get("station_code", "")
            if station in station_encoder:
                station_encoded = station_encoder[station]
            else:
                continue  # skip unseen stations

            velocity = rec.get("velocity")
            if velocity is None:
                velocity = median_velocity

            stop_number = rec.get("stop_number", 1)

            # build feature vector in the same order as training
            features = np.array([[
                route_encoded, station_encoded, hour,
                day_of_week, is_weekend, stop_number, velocity
            ]])

            predicted_delay = model.predict(features)[0]
            actual_delay = rec.get("delay_arrival_min")

            error = None
            if actual_delay is not None:
                error = round(predicted_delay - actual_delay, 1)

            predictions.append({
                "scrape_time": scrape_time,
                "train_number": rec.get("train_number", ""),
                "route_name": route,
                "station_code": station,
                "station_name": rec.get("station_name", ""),
                "predicted_delay_min": round(predicted_delay, 1),
                "actual_delay_min": actual_delay,
                "prediction_error": error,
                "model_version": model_data.get("trained_at", "unknown")
            })

        except Exception:
            continue

    return predictions


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
    all_records = []  # collect for ML predictions

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
                INSERT INTO routes (route_name, origin, destination, first_seen, last_seen)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(route_name) DO UPDATE SET
                    last_seen = excluded.last_seen
            """, (route_name, orig, dest, scrape_time, scrape_time))

            # process each station stop
            stop_num = 0
            for station in train.get("stations", []):
                stop_num += 1
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

                # collect departed records for prediction
                if status == "Departed" and delay_arr is not None:
                    all_records.append({
                        "train_number": train_number,
                        "route_name": route_name,
                        "station_code": code,
                        "station_name": name,
                        "scheduled_arrival": sch_arr,
                        "delay_arrival_min": delay_arr,
                        "velocity": velocity,
                        "stop_number": stop_num,
                    })

            trains_processed += 1

    conn.commit()
    return trains_processed, stations_logged, all_records, scrape_time


def store_predictions(conn, predictions):
    """Store ML predictions in the predictions table."""
    cursor = conn.cursor()
    for p in predictions:
        cursor.execute("""
            INSERT INTO predictions (
                scrape_time, train_number, route_name, station_code,
                station_name, predicted_delay_min, actual_delay_min,
                prediction_error, model_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            p["scrape_time"], p["train_number"], p["route_name"],
            p["station_code"], p["station_name"],
            p["predicted_delay_min"], p["actual_delay_min"],
            p["prediction_error"], p["model_version"]
        ))
    conn.commit()


def main():
    """Main entry point: scrape → store → predict."""
    print(f"[{datetime.now().isoformat()}] Starting Amtrak data collection...")

    conn = sqlite3.connect(DB_PATH)
    create_tables(conn)

    try:
        # Step 1: Scrape and store
        trains, stations, records, scrape_time = fetch_and_store(conn)
        print(f"  ✓ Collected data for {trains} trains, {stations} station stops")

        # Step 2: Run ML predictions (if model exists)
        model_data = load_model()
        if model_data and len(records) > 0:
            predictions = predict_delays(model_data, records, scrape_time)
            if predictions:
                store_predictions(conn, predictions)
                # compute summary stats
                errors = [p["prediction_error"] for p in predictions if p["prediction_error"] is not None]
                if errors:
                    mae = np.mean(np.abs(errors))
                    print(f"  ✓ ML predictions: {len(predictions)} stations, MAE = {mae:.1f} min")
                else:
                    print(f"  ✓ ML predictions: {len(predictions)} stations (no actuals to compare)")
        elif model_data is None:
            print("  ℹ No ML model found — run the notebook to train and export one")

    except requests.RequestException as e:
        print(f"  ✗ API request failed: {e}")
    except Exception as e:
        print(f"  ✗ Error: {e}")
    finally:
        conn.close()

    print(f"[{datetime.now().isoformat()}] Done.\n")


if __name__ == "__main__":
    main()
