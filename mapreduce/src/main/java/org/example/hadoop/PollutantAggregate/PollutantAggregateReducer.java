package org.example.hadoop.PollutantAggregate;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PollutantAggregateReducer extends Reducer<Text, Text, NullWritable, Text> {

    private static final String KEY_VALUE_SEPARATOR = "#KV#";
    private static final String PAYLOAD_SEPARATOR_REGEX = "#P#"; // Regex for splitting
    private static final String POLLUTANT_VALUE_SEPARATOR_REGEX = ":PV:"; // Regex for splitting
    private static final String OUTPUT_DELIMITER = "\t";

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String[] keyParts = key.toString().split(KEY_VALUE_SEPARATOR);
        if (keyParts.length != 2) {
            System.err.println("Skipping malformed key: " + key.toString());
            return;
        }
        String actualStationID = keyParts[0];
        String actualTime = keyParts[1];

        Map<String, String> pollutantValues = new HashMap<>();
        pollutantValues.put("C6H6", "NA");
        pollutantValues.put("PM10", "NA");
        pollutantValues.put("PM25", "NA");
        pollutantValues.put("NO2", "NA");
        pollutantValues.put("SO2", "NA");

        String lat = "NA";
        String lon = "NA";
        String state = "NA";
        String city = "NA";
        boolean metadataSet = false;

        for (Text val : values) {
            // Value: pollutantType:PV:measurementValue#P#lat#P#long#P#state#P#city
            String valueStr = val.toString();
            String[] payloadParts = valueStr.split(PAYLOAD_SEPARATOR_REGEX);

            if (payloadParts.length < 2) { // At least pollutant+value and one metadata part
                System.err.println("Skipping malformed payload value: " + valueStr + " for key: " + key.toString());
                continue;
            }

            String[] pollutantPart = payloadParts[0].split(POLLUTANT_VALUE_SEPARATOR_REGEX);
            if (pollutantPart.length == 2) {
                String type = pollutantPart[0];
                String measurement = pollutantPart[1];
                pollutantValues.put(type, measurement);
            } else {
                System.err.println("Skipping malformed pollutant part: " + payloadParts[0] + " for key: " + key.toString());
            }

            if (!metadataSet && payloadParts.length >= 5) { // pollutant, lat, lon, state, city
                lat = payloadParts[1];
                lon = payloadParts[2];
                state = payloadParts[3];
                city = payloadParts[4];
                metadataSet = true;
            }
        }

        // Construct the final output string
        // Lat Long StationId State City Time C6H6 PM10 PM25 NO2 SO2
        StringBuilder sb = new StringBuilder();
        sb.append(lat).append(OUTPUT_DELIMITER);
        sb.append(lon).append(OUTPUT_DELIMITER);
        sb.append(actualStationID).append(OUTPUT_DELIMITER);
        sb.append(state).append(OUTPUT_DELIMITER);
        sb.append(city).append(OUTPUT_DELIMITER);
        sb.append(actualTime).append(OUTPUT_DELIMITER);
        sb.append(pollutantValues.get("C6H6")).append(OUTPUT_DELIMITER);
        sb.append(pollutantValues.get("PM10")).append(OUTPUT_DELIMITER);
        sb.append(pollutantValues.get("PM25")).append(OUTPUT_DELIMITER);
        sb.append(pollutantValues.get("NO2")).append(OUTPUT_DELIMITER);
        sb.append(pollutantValues.get("SO2")); // No trailing delimiter

        context.write(NullWritable.get(), new Text(sb.toString()));
        context.getCounter("ReducerCounters", "FinalRowsEmitted").increment(1);
    }
}
