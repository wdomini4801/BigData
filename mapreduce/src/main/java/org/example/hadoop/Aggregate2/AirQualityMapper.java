package org.example.hadoop.Aggregate2;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class AirQualityMapper extends Mapper<LongWritable, Text, Text, Text> {

  private Pattern stationIdPattern = Pattern.compile("^[A-Za-z]+");
  private long maxRecordsToProcess = Long.MAX_VALUE;
  private long recordsProcessed = 0;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    // Get debug limit from configuration
    Configuration conf = context.getConfiguration();
    maxRecordsToProcess = conf.getLong("debug.max.records", Long.MAX_VALUE);
  }

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    if (recordsProcessed >= maxRecordsToProcess) {
      context.getCounter(ProcessingCounters.SKIPPED_RECORDS).increment(1);
      return;
    }

    String line = value.toString().trim();
    if (line.isEmpty()) {
      context.getCounter(ProcessingCounters.SKIPPED_RECORDS).increment(1);
      return;
    }

    String filename = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) context.getInputSplit())
                        .getPath().getName().toUpperCase(); // uppercase for case-insensitive check

    if (filename.contains("METADATA")) {
      processMetadata(line, context);
      context.getCounter(ProcessingCounters.METADATA_RECORDS_PROCESSED).increment(1);
    } else {
      // Check if filename contains any pollutant substring
      String[] pollutants = { "PM10", "PM25", "SO2", "NO2", "C6H6" };
      boolean isMeasurementFile = false;
      for (String pollutant : pollutants) {
        if (filename.contains(pollutant)) {
          isMeasurementFile = true;
          break;
        }
      }

      if (isMeasurementFile) {
        processMeasurements(line, context, filename);
        context.getCounter(ProcessingCounters.MEASUREMENT_RECORDS_PROCESSED).increment(1);
        recordsProcessed++;
      } else {
        // skip files that are neither metadata nor measurement files
        context.getCounter(ProcessingCounters.SKIPPED_RECORDS).increment(1);
      }
    }
  }

  private void processMetadata(String line, Context context)
      throws IOException, InterruptedException {

    // Skip header
    if (line.startsWith("Number;StationID"))
      return;

    String[] fields = line.split(";");
    if (fields.length >= 15) {
      String stationId = fields[1]; // StationID
      String internationalId = fields[2]; // InternationalStationID
      String lat = fields[13]; // lat
      String lon = fields[14]; // long

      // Emit metadata with "META:" prefix
      String metadataValue = String.format("META:%s,%s,%s,%s",
          stationId, internationalId, lat, lon);
      context.write(new Text(stationId), new Text(metadataValue));
    }
  }

  private void processMeasurements(String line, Context context, String filename)
      throws IOException, InterruptedException {

    String[] fields = line.split(",");
    if (fields.length < 2)
      return;

    // Process header to extract station IDs and pollutant types
    if (line.startsWith("Time,")) {
      context.getCounter(ProcessingCounters.HEADER_RECORDS_PROCESSED).increment(1);
      for (int i = 1; i < fields.length; i++) {
        String columnHeader = fields[i].trim();
        String[] parts = columnHeader.split("-");
        if (parts.length >= 2) {
          String rawStationId = parts[0];
          String pollutantType = parts[1];

          // Clean station ID (extract alphabetic part)
          Matcher matcher = stationIdPattern.matcher(rawStationId);
          String cleanedStationId = matcher.find() ? matcher.group(0) : rawStationId;

          // Store column mapping for this mapper instance
          String mappingKey = String.format("MAPPING:%d", i);
          String mappingValue = String.format("%s,%s", cleanedStationId, pollutantType);
          context.write(new Text(mappingKey), new Text(mappingValue));
          context.getCounter(ProcessingCounters.STATION_ID_MATCHES_FOUND).increment(1);
        }
      }
      return;
    }

    // Process data rows
    if (!fields[0].trim().isEmpty() && !fields[0].equals("Time")) {
      String timestamp = fields[0].trim();

      for (int i = 1; i < fields.length; i++) {
        String value = fields[i].trim();
        if (!value.isEmpty() && !value.equals("")) {
          // We'll need to match this index with our mapping
          // For now, emit with index as key for later processing
          String dataKey = String.format("DATA:%s:%d", timestamp, i);
          context.write(new Text(dataKey), new Text(value));
        } else {
          context.getCounter(ProcessingCounters.EMPTY_VALUES_FOUND).increment(1);
        }
      }
    }
  }
}
