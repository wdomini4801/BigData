import csv
import requests
import time
import os

# Configuration
input_file = './data/stations_lat_long.csv'
base_output_dir = "./output"
year = 2018
output_dir = os.path.join(base_output_dir, str(year))
os.makedirs(output_dir, exist_ok=True)

rate_limit = 130  # Max number of API calls allowed in a single run
api_base_url = "https://archive-api.open-meteo.com/v1/archive"
timezone = "Europe%2FBerlin"
data_params = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "surface_pressure",
    "wind_speed_10m",
    "wind_direction_10m",
    "direct_radiation"
]

# Load station coordinates
stations = []
with open(input_file, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        stations.append({
            'OriginalID': row['OriginalID'],
            'Latitude': row['Latitude'],
            'Longitude': row['Longitude']
        })

# Get list of already downloaded files
existing_files = set(os.listdir(output_dir))

# Query API and save responses
calls_made = 0
for station in stations:
    filename = f"openmeteo_{station['OriginalID']}_{year}.csv"
    if filename in existing_files:
        print(f"Skipping {station['OriginalID']} (already downloaded)")
        continue

    if calls_made >= rate_limit:
        print("Rate limit reached. Exiting.")
        break

    url = f"{api_base_url}?latitude={station['Latitude']}"
    url += f"&longitude={station['Longitude']}"
    url += f"&start_date={year}-01-01&end_date={year}-12-31"
    url += f"&hourly={','.join(data_params)}"
    url += f"&timezone={timezone}"
    url += "&format=csv"

    try:
        response = requests.get(url)
        response.raise_for_status()
        with open(os.path.join(output_dir, filename), 'w', encoding='utf-8') as f:
            f.write(response.text)
        print(f"Saved data for {station['OriginalID']}")
        calls_made += 1
        time.sleep(1)  # Small delay to avoid hitting rate limits
    except requests.RequestException as e:
        print(f"Failed to fetch data for {station['OriginalID']}: {e}")
