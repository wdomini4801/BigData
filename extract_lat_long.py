import csv
import re

# Input files
pm10_file = './data/2017_PM10_1g.csv'
metadata_file = './data/stations_metadata.csv'
output_file = './data/stations_lat_long.csv'

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
            print(f"Warning: Cleaned StationID {cleaned_id} (from {original_id}) not found in metadata.")