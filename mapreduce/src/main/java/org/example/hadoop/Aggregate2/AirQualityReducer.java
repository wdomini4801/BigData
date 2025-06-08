package org.example.hadoop.Aggregate2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.example.hadoop.Aggregate2.Models.StationMetadata;

public class AirQualityReducer extends Reducer<Text, Text, Text, Text> {

    private final Map<String, StationMetadata> stationMetadata = new HashMap<>();
    private final Map<Integer, String[]> columnMappings = new HashMap<>();
    private final Map<String, Map<String, String>> dataByStationTimestamp = new HashMap<>();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String keyStr = key.toString();

        // Metadata entry
        if (!keyStr.startsWith("MAPPING:") && !keyStr.startsWith("DATA:")) {
            for (Text value : values) {
                if (value.toString().startsWith("META:")) {
                    String[] parts = value.toString().substring(5).split(",");
                    if (parts.length >= 4) {
                        stationMetadata.put(keyStr, new StationMetadata(
                                parts[0], parts[1], parts[2], parts[3]));
                    }
                }
            }
        }

        // Column mapping
        else if (keyStr.startsWith("MAPPING:")) {
            int colIndex = Integer.parseInt(keyStr.split(":")[1]);
            for (Text value : values) {
                String[] parts = value.toString().split(",");
                if (parts.length >= 2) {
                    columnMappings.put(colIndex, new String[] { parts[0], parts[1] }); // stationId, pollutant
                }
            }
        }

        // Data values
        else if (keyStr.startsWith("DATA:")) {
            String[] keyParts = keyStr.split(":");
            if (keyParts.length == 3) {
                String timestamp = keyParts[1];
                int colIndex = Integer.parseInt(keyParts[2]);

                for (Text value : values) {
                    String val = value.toString();
                    String[] mapping = columnMappings.get(colIndex);
                    if (mapping != null) {
                        String stationId = mapping[0];
                        String pollutant = mapping[1];

                        String compositeKey = timestamp + "_" + stationId;
                        dataByStationTimestamp.putIfAbsent(compositeKey, new HashMap<>());
                        dataByStationTimestamp.get(compositeKey).put(pollutant, val);
                    }
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Emit metadata
        for (Map.Entry<String, StationMetadata> entry : stationMetadata.entrySet()) {
            StationMetadata meta = entry.getValue();
            String stationKey = "STATION:" + meta.getStationId();
            String metaValue = String.format("META:%s,%s,%s,%s",
                    meta.getStationId(), meta.getInternationalStationId(),
                    meta.getLatitude(), meta.getLongitude());

            context.write(new Text(stationKey), new Text(metaValue));
        }

        // Emit data
        for (Map.Entry<Integer, String[]> mappingEntry : columnMappings.entrySet()) {
            String[] mapping = mappingEntry.getValue(); // [stationId, pollutant]
            String stationId = mapping[0];
            String pollutant = mapping[1];

            for (Map.Entry<String, Map<String, String>> dataEntry : dataByStationTimestamp.entrySet()) {
                String compositeKey = dataEntry.getKey(); // timestamp_stationId
                String[] parts = compositeKey.split("_", 2);
                if (parts.length != 2) continue;

                String timestamp = parts[0];
                String station = parts[1];

                if (!station.equals(stationId)) continue;

                Map<String, String> pollutantValues = dataEntry.getValue();
                if (pollutantValues.containsKey(pollutant)) {
                    String value = pollutantValues.get(pollutant);
                    String data = String.format("DATA:%s,%s,%s", timestamp, pollutant, value);
                    context.write(new Text("STATION:" + stationId), new Text(data));
                }
            }
        }
    }

}
