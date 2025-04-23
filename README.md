
# Data files
```./data/```
- ```2017_PM10_1g.csv```
- ```stations_lat_long.csv```
- ```stations_metadata.csv```

# Config files
```./config/```
- ```hadoop_config.json```
### Content:
```json
{
  "ssh_user": "<username>",
  "ssh_host": "<instance ip>",
  "ssh_port": 22,
  "private_key_path": "<path to id_rsa>"
}

```

# Scripts

1. ```extract_lat_long.py``` - extracts lat and long from ```./data/stations_metadata.csv```, makes sure there are readings of pm10 from ```2017_PM10_1g.csv``` from and saves into ```./data/stations_lat_long.csv```. 

    Format:
    ```csv
    OriginalID,MatchedStationID,Latitude,Longitude,Number,InternationalStationID
    DsBoleslaMOB,DsBoleslaMOB,51.263245,15.570354,6,PL0658A
    ```


2. ```download_open_meteo.py``` - downloads historical weather from the free API. It will download only missing files from selected year and ```./data/stations_lat_long.csv``` as input.

