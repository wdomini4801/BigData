import csv
import re
import requests
import time
import os

def extract_lat_lon(pm10_file, metadata_file, output_file, logger):

  # Extract cleaned station IDs from the PM10 header
  with open(pm10_file, 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    header = next(reader)
    raw_ids = [col.split('-')[0] for col in header[1:]]  # Skip 'Time' column
    station_ids = [re.match(r'^[A-Za-z]+', station_id).group(0) if re.match(r'^[A-Za-z]+', station_id) else station_id for station_id in raw_ids]

  # Read metadata and store lat/long and IDs by StationID
  metadata = {}
  with open(metadata_file, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f, delimiter=';')
    for row in reader:
      metadata[row['StationID']] = {
        'lat': row['lat'],
        'long': row['long'],
        'Number': row['Number'],
        'InternationalStationID': row['InternationalStationID']
      }

  # Match and write to output file
  with open(output_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['OriginalID', 'MatchedStationID', 'Latitude', 'Longitude', 'Number', 'InternationalStationID'])

    for original_id, cleaned_id in zip(raw_ids, station_ids):
      matched = False
      for station_meta_id in metadata:
        if cleaned_id in station_meta_id:
          data = metadata[station_meta_id]
          writer.writerow([original_id, station_meta_id, data['lat'], data['long'], data['Number'], data['InternationalStationID']])
          matched = True
          break
      if not matched:
        logger.info(f"Warning: Cleaned StationID {cleaned_id} (from {original_id}) not found in metadata.")

def download_open_meteo_yearly_measurements(stations_file_path, base_output_dir, year, rate_limit, logger, fail_limit=10, request_delay=0.2):
  output_dir = os.path.join(base_output_dir, str(year))
  os.makedirs(output_dir, exist_ok=True)

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
  with open(stations_file_path, 'r', encoding='utf-8') as f:
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
  skipped_calls = 0
  num_fails = 0
  run_was_successful = True
  for station in stations:
    calls_made += 1
    filename = f"openmeteo_{station['OriginalID']}_{year}.csv"
    if filename in existing_files:
      skipped_calls += 1
      continue

    if calls_made >= rate_limit:
      logger.warning("Rate limit reached. Exiting.")
      break

    url = create_request_url(year, api_base_url, timezone, data_params, station)

    try:
      response = requests.get(url)
      response.raise_for_status()
      with open(os.path.join(output_dir, filename), 'w', encoding='utf-8') as f:
        f.write(response.text)
      # logger.info(f"Saved data for {station['OriginalID']}")
      print(f"Skipped: {skipped_calls} Downloaded: {calls_made}/{rate_limit}", end='\r')
      time.sleep(request_delay)  # Small delay to avoid hitting rate limits
    except requests.RequestException as e:
      run_was_successful = False
      num_fails += 1
      # logger.error(f"Failed to fetch data for {station['OriginalID']}")
      if num_fails >= fail_limit:
        return False
      
  return run_was_successful

def create_request_url(year, api_base_url, timezone, data_params, station):
  url = f"{api_base_url}?latitude={station['Latitude']}"
  url += f"&longitude={station['Longitude']}"
  url += f"&start_date={year}-01-01&end_date={year}-12-31"
  url += f"&hourly={','.join(data_params)}"
  url += f"&timezone={timezone}"
  url += "&format=csv"
  return url
