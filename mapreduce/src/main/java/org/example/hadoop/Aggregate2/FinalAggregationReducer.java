package org.example.hadoop.Aggregate2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.example.hadoop.Aggregate2.Models.StationMetadata;

public class FinalAggregationReducer extends Reducer<Text, Text, Text, Text> {

  private Map<String, StationMetadata> stationMetadata = new HashMap<>();

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    String keyStr = key.toString();

    if (keyStr.startsWith("STATION:")) {
      String stationId = keyStr.substring(8);
      Map<String, String> pollutantValues = new HashMap<>();
      StationMetadata metadata = null;
      String timestamp = "";

      for (Text value : values) {
        String valueStr = value.toString();
        if (valueStr.startsWith("META:")) {
          String[] parts = valueStr.substring(5).split(",");
          if (parts.length >= 4) {
            metadata = new StationMetadata(parts[0], parts[1], parts[2], parts[3]);
          }
        } else if (valueStr.startsWith("DATA:")) {
          String[] parts = valueStr.substring(5).split(",");
          if (parts.length >= 3) {
            timestamp = parts[0];
            pollutantValues.put(parts[1], parts[2]);
          }
        }
      }

      if (metadata != null && !pollutantValues.isEmpty()) {
        // Build output row
        StringBuilder output = new StringBuilder();
        output.append(timestamp).append(",");
        output.append(metadata.getStationId()).append(",");
        output.append(metadata.getInternationalStationId()).append(",");
        output.append(metadata.getLatitude()).append(",");
        output.append(metadata.getLongitude()).append(",");

        // Add pollutant values in fixed order
        String[] pollutants = { "PM10", "PM25", "SO2", "NO2", "C6H6" };
        for (int i = 0; i < pollutants.length; i++) {
          if (i > 0)
            output.append(",");
          output.append(pollutantValues.getOrDefault(pollutants[i], ""));
        }

        context.write(new Text(timestamp + "_" + stationId), new Text(output.toString()));
      }
    }
  }
}