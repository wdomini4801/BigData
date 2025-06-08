package org.example.hadoop.Aggregate2;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.example.hadoop.Aggregate2.Models.MeasurementData;
import org.example.hadoop.Aggregate2.Models.StationMetadata;

public class JoinReducer extends Reducer<Text, MeasurementData, NullWritable, Text> {
  private Map<String, StationMetadata> stationMap = new HashMap<>();

  enum CountersEnum {
    MISSING_METADATA,
    JOINED_RECORDS
  }

  @Override
  protected void setup(Context context) throws IOException {
    URI[] cacheFiles = context.getCacheFiles();
    if (cacheFiles == null || cacheFiles.length == 0) {
      throw new FileNotFoundException("Station metadata file not found in distributed cache.");
    }

    for (URI uri : cacheFiles) {
      Path path = new Path("station_metadata.csv");
      try (BufferedReader br = new BufferedReader(new FileReader(path.toString()))) {
        String line = br.readLine(); // Skip header
        while ((line = br.readLine()) != null) {
          String[] fields = line.split(";");
          if (fields.length < 15) continue;

          String stationId = fields[1];
          String internationalId = fields[2];
          String lat = fields[13];
          String lon = fields[14];

          stationMap.put(stationId, new StationMetadata(stationId, internationalId, lat, lon));
        }
      }
    }
  }

  @Override
  public void reduce(Text key, Iterable<MeasurementData> values, Context context)
      throws IOException, InterruptedException {
    String stationId = key.toString();

    StationMetadata metadata = null;
    for (String metaKey : stationMap.keySet()) {
      if (metaKey.contains(stationId)) {
        metadata = stationMap.get(metaKey);
        break;
      }
    }

    if (metadata == null) {
      context.getCounter(CountersEnum.MISSING_METADATA).increment(1);
      return;
    }

    for (MeasurementData md : values) {
      String result = String.join(",",
        md.getTimestamp(),
        md.getStationId(),
        md.getPollutantType(),
        md.getValue(),
        metadata.getLatitude(),
        metadata.getLongitude(),
        metadata.getInternationalStationId()
      );
      context.write(NullWritable.get(), new Text(result));
      context.getCounter(CountersEnum.JOINED_RECORDS).increment(1);
    }
  }
}
