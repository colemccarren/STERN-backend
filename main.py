import os
import pandas as pd
import zipfile
from datetime import datetime, timedelta
from io import BytesIO
import requests
from flask import Flask, request, jsonify

# --- This is your Flask app setup ---
app = Flask(__name__)

# --- Paste ALL your GTFS functions from Cell 2 here ---
# (read_gtfs_from_zip, parse_gtfs_time, get_active_services_on_date, calculate_service_hours_from_url)

def read_gtfs_from_zip(zip_file_like_object):
    """Reads GTFS files from a zip file-like object."""
    with zipfile.ZipFile(zip_file_like_object, 'r') as z:
        gtfs_files = {}
        for name in z.namelist():
            if name.endswith('.txt'):
                try:
                    with z.open(name) as f:
                        gtfs_files[name] = pd.read_csv(f, dtype=str)
                except UnicodeDecodeError:
                    print(f"UnicodeDecodeError reading {name}, trying latin1...")
                    with z.open(name) as f:
                        gtfs_files[name] = pd.read_csv(f, dtype=str, encoding='latin1')
                except Exception as e:
                    print(f"Could not read {name}: {e}")
                    gtfs_files[name] = pd.DataFrame()
    return gtfs_files

def parse_gtfs_time(t):
    """Parses GTFS time string (HH:MM:SS) into a timedelta object."""
    if pd.isna(t):
        return None
    try:
        h, m, s = map(int, t.split(":"))
        return timedelta(hours=h, minutes=m, seconds=s)
    except ValueError:
        return None

def get_active_services_on_date(target_date, calendar, calendar_dates):
    """
    Determines active service_ids for a given date based on calendar.txt and calendar_dates.txt.
    """
    weekday = target_date.strftime("%A").lower()
    target_date_yyyymmdd = target_date.strftime("%Y%m%d")
    target_date_pd = pd.Timestamp(target_date.year, target_date.month, target_date.day)

    base_services = set()
    if not calendar.empty and all(col in calendar.columns for col in [weekday, "start_date", "end_date", "service_id"]):
        if not pd.api.types.is_datetime64_any_dtype(calendar["start_date"]):
            calendar["start_date"] = pd.to_datetime(calendar["start_date"], format="%Y%m%d", errors='coerce')
        if not pd.api.types.is_datetime64_any_dtype(calendar["end_date"]):
            calendar["end_date"] = pd.to_datetime(calendar["end_date"], format="%Y%m%d", errors='coerce')

        active_calendar = calendar[
            (calendar[weekday] == '1') &
            (calendar["start_date"] <= target_date_pd) &
            (calendar["end_date"] >= target_date_pd)
        ]
        base_services.update(active_calendar["service_id"].tolist())

    added_services = set()
    removed_services = set()
    if not calendar_dates.empty and all(col in calendar_dates.columns for col in ["date", "exception_type", "service_id"]):
        if pd.api.types.is_datetime64_any_dtype(calendar_dates["date"]):
             calendar_dates["date_str"] = calendar_dates["date"].dt.strftime("%Y%m%d")
        else:
             calendar_dates["date_str"] = calendar_dates["date"].astype(str)

        added_services.update(calendar_dates[
            (calendar_dates["date_str"] == target_date_yyyymmdd) &
            (calendar_dates["exception_type"] == '1')
        ]["service_id"].tolist())

        removed_services.update(calendar_dates[
            (calendar_dates["date_str"] == target_date_yyyymmdd) &
            (calendar_dates["exception_type"] == '2')
        ]["service_id"].tolist())

    active_services = (base_services.union(added_services)) - removed_services
    return list(active_services)

def calculate_service_hours_from_url(gtfs_zip_url, start_date_str, end_date_str):
    """
    Downloads a GTFS zip file from a URL and calculates total service hours.
    """
    print(f"Attempting to download GTFS data from: {gtfs_zip_url}")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    try:
        response = requests.get(gtfs_zip_url, headers=headers, timeout=60)
        response.raise_for_status()
        print("Download successful.")
    except Exception as e:
        print(f"Error downloading GTFS zip file: {e}")
        return None

    gtfs_zip_file_like_object = BytesIO(response.content)
    gtfs_data = read_gtfs_from_zip(gtfs_zip_file_like_object)
    
    # ... (rest of your validation and processing logic) ...
    
    required_files = ["calendar.txt", "trips.txt", "stop_times.txt"]
    if "calendar_dates.txt" not in gtfs_data:
        print("Warning: calendar_dates.txt not found.")
        gtfs_data["calendar_dates.txt"] = pd.DataFrame(columns=["service_id", "date", "exception_type"])

    for req_file in required_files:
        if req_file not in gtfs_data or gtfs_data[req_file].empty:
            if req_file == "calendar.txt" and "calendar_dates.txt" in gtfs_data and not gtfs_data["calendar_dates.txt"].empty:
                print(f"Warning: {req_file} is missing, proceeding.")
                gtfs_data[req_file] = pd.DataFrame(columns=["service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "start_date", "end_date"])
            else:
                print(f"Error: Essential GTFS file '{req_file}' is missing or empty.")
                return 0.0

    calendar = gtfs_data["calendar.txt"]
    calendar_dates = gtfs_data["calendar_dates.txt"]
    trips = gtfs_data["trips.txt"]
    stop_times = gtfs_data["stop_times.txt"]

    # ... (rest of your preprocessing logic) ...
    if "start_date" not in calendar.columns: calendar["start_date"] = pd.Series(dtype='datetime64[ns]')
    if "end_date" not in calendar.columns: calendar["end_date"] = pd.Series(dtype='datetime64[ns]')
    if "start_date" in calendar.columns and not pd.api.types.is_datetime64_any_dtype(calendar["start_date"]):
        calendar["start_date"] = pd.to_datetime(calendar["start_date"], format="%Y%m%d", errors='coerce')
    if "end_date" in calendar.columns and not pd.api.types.is_datetime64_any_dtype(calendar["end_date"]):
        calendar["end_date"] = pd.to_datetime(calendar["end_date"], format="%Y%m%d", errors='coerce')
    stop_times["arrival_time"] = stop_times["arrival_time"].fillna(stop_times["departure_time"])
    stop_times["departure_time"] = stop_times["departure_time"].fillna(stop_times["arrival_time"])
    if 'arrival_time' not in stop_times.columns or 'departure_time' not in stop_times.columns: return 0.0

    start_date_dt = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date_dt = datetime.strptime(end_date_str, "%Y-%m-%d")
    target_dates = pd.date_range(start=start_date_dt, end=end_date_dt)
    total_hours = 0.0

    for date_obj in target_dates:
        active_services = get_active_services_on_date(date_obj, calendar, calendar_dates)
        if not active_services: continue
        trips_on_date = trips[trips["service_id"].isin(active_services)]
        if trips_on_date.empty: continue
        if 'trip_id' not in trips_on_date.columns or 'trip_id' not in stop_times.columns: continue

        merged = trips_on_date.merge(stop_times, on="trip_id", how="inner")
        if merged.empty: continue
        
        merged["arrival_td"] = merged["arrival_time"].apply(parse_gtfs_time)
        merged["departure_td"] = merged["departure_time"].apply(parse_gtfs_time)
        merged.dropna(subset=["arrival_td", "departure_td"], inplace=True)
        if merged.empty: continue

        if "block_id" not in merged.columns or merged["block_id"].isnull().all():
            for trip_id, group in merged.groupby("trip_id"):
                trip_start_td = group["departure_td"].min()
                trip_end_td = group["arrival_td"].max()
                if pd.notnull(trip_start_td) and pd.notnull(trip_end_td) and trip_end_td > trip_start_td:
                    total_hours += (trip_end_td - trip_start_td).total_seconds() / 3600.0
            continue

        for block_id, group in merged.groupby("block_id"):
            if group.empty: continue
            block_start_td = group["departure_td"].min()
            block_end_td = group["arrival_td"].max()
            if pd.notnull(block_start_td) and pd.notnull(block_end_td) and block_end_td > block_start_td:
                total_hours += (block_end_td - block_start_td).total_seconds() / 3600.0

    return total_hours


# --- This is your API endpoint from Cell 3 ---
@app.route('/calculate_hours', methods=['POST'])
def calculate_endpoint():
    try:
        data = request.get_json()
        agency_key = data.get('agencyKey')
        start_date_iso = data.get('startDate')
        end_date_iso = data.get('endDate')
        
        start_date_str = pd.to_datetime(start_date_iso).strftime('%Y-%m-%d')
        end_date_str = pd.to_datetime(end_date_iso).strftime('%Y-%m-%d')

        if not all([agency_key, start_date_str, end_date_str]):
            return jsonify({"error": "Missing parameters (agencyKey, startDate, endDate)"}), 400

        gtfs_url = f"https://gtfs-intake-prod.swiftly-internal.com/uploads/{agency_key}/latest.zip"
        print(f"Received job: Processing {gtfs_url} from {start_date_str} to {end_date_str}")
        
        total_hours = calculate_service_hours_from_url(gtfs_url, start_date_str, end_date_str)

        if total_hours is None:
             print("Calculation failed. See logs above.")
             return jsonify({"error": "Calculation failed. Check server logs."}), 500

        print(f"Calculation complete. Total hours: {total_hours}")
        
        return jsonify({
            "revenue_hours": round(total_hours, 2),
            "processed_on": "cloud_run",
            "agency_key": agency_key,
            "start_date": start_date_str,
            "end_date": end_date_str
        })
        
    except Exception as e:
        print(f"An error occurred in the API endpoint: {str(e)}")
        return jsonify({"error": str(e)}), 500

# --- This part tells Cloud Run how to start the server ---
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))