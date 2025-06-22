package org.example.hadoop.Aggregate2;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.example.hadoop.Aggregate2.Models.MeasurementData;

public class MeasurementMapper extends Mapper<LongWritable, Text, Text, MeasurementData> {
  private String[] headers;
  private boolean isHeader = true;


  //DEBUG
  private static final boolean DEBUG_LIMIT_ENABLED = false;
  private static final int DEBUG_LIMIT = 100;
  private int lineCounter = 0;

  // STATION FILTER
  private static final boolean RUN_ONLY_FILTER_ENABLED = true;
  private static final Set<String> RUN_ONLY = new HashSet<>(Arrays.asList(
    "DsJelGorOgin", "DsWrocWybCon", "KpBydPlPozna", "KpBydWarszaw", "LdLodzGdansk"
  ));

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    if (DEBUG_LIMIT_ENABLED && lineCounter >= DEBUG_LIMIT + 1) return;

    String line = value.toString();
    String[] fields = line.split(",");

    if (isHeader) {
      headers = fields;
      isHeader = false;
      return;
    }

    lineCounter++;

    String timestamp = fields[0];
    for (int i = 1; i < fields.length; i++) {
      String header = headers[i]; // e.g., DsBoleslaMOB-PM10-1g

      if (header == null || header.trim().isEmpty()) continue;

      String[] parts = header.split("-");
      if (parts.length < 2) continue;

      String rawStationId = parts[0];
      String pollutant = parts[1].replaceAll("\\d+g$", ""); // PM10-1g → PM10

      // APPLY FILTER
      if (RUN_ONLY_FILTER_ENABLED && !RUN_ONLY.contains(rawStationId)) continue;

      String valueStr = fields[i];
      if (valueStr != null && !valueStr.isEmpty()) {
        context.write(
          new Text(rawStationId),
          new MeasurementData(timestamp, rawStationId, pollutant, valueStr)
        );
      }
    }
  }
}
