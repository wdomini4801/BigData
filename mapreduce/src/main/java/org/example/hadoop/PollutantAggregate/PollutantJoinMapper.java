package org.example.hadoop.PollutantAggregate;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.example.hadoop.Models.StationInfo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PollutantJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Map<String, StationInfo> stationMetadataMap = new HashMap<>();
    private Map<Integer, String> columnIndexToCleanStationID = new HashMap<>();
    private String currentPollutantType = "UNKNOWN";
    private boolean headerProcessed = false;
    private static final String METADATA_DELIMITER = ";";
    private static final String POLLUTION_DELIMITER = ",";
    private static final String OUTPUT_DELIMITER = "\t"; // For the final output structure
    private static final String KEY_VALUE_SEPARATOR = "#KV#"; // Separator for key parts
    private static final String PAYLOAD_SEPARATOR = "#P#";   // Separator for payload parts
    private static final String POLLUTANT_VALUE_SEPARATOR = ":PV:"; // Separator for pollutant type and value
    private Pattern stationIdPattern = Pattern.compile("^([^-]+)-(" + String.join("|", "C6H6", "NO2", "PM10", "PM2.5", "SO2") + ")-.*$");

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Load station metadata from DistributedCache
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            for (URI cacheFile : cacheFiles) {
                // Assuming the symlink name or filename helps identify the metadata file
                if (cacheFile.getPath().endsWith("stations-metadata.csv")) { // Adjust if using symlink
                    loadStationMetadata(new Path(cacheFile.getPath()), context);
                    break;
                }
            }
        } else {
            throw new IOException("Station metadata file not found in DistributedCache.");
        }

        // Determine pollutant type from input file name
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        if (fileName.contains("C6H6")) currentPollutantType = "C6H6";
        else if (fileName.contains("NO2")) currentPollutantType = "NO2";
        else if (fileName.contains("PM10")) currentPollutantType = "PM10";
        else if (fileName.contains("PM25")) currentPollutantType = "PM25";
        else if (fileName.contains("SO2")) currentPollutantType = "SO2";
        else {
            System.err.println("Warning: Could not determine pollutant type for file: " + fileName);
        }
        headerProcessed = false; // Reset for each new file split (though usually one mapper per split)
        columnIndexToCleanStationID.clear();
    }

    private void loadStationMetadata(Path filePath, Context context) throws IOException {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(filePath.toString()));
            String line;
            boolean isHeader = true;
            while ((line = reader.readLine()) != null) {
                if (isHeader) {
                    isHeader = false;
                    continue; // Skip header line of metadata
                }
                String[] parts = line.split(METADATA_DELIMITER, -1); // -1 to keep trailing empty strings
                if (parts.length >= 15) { // Ensure enough columns
                    // Number;StationID;...;State;City;Address;lat;long;
                    // Indices: 0      1         10    11     12     13  14
                    String stationID = parts[1].trim();
                    String state = parts[10].trim();
                    String city = parts[11].trim();
                    String lat = parts[13].trim();
                    String lon = parts[14].trim();

                    if (!stationID.isEmpty()) {
                        stationMetadataMap.put(stationID, new StationInfo(stationID, lat, lon, state, city));
                    }
                } else {
                    System.err.println("Skipping malformed metadata line: " + line);
                }
            }
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
        context.getCounter("MapperCounters", "StationMetadataLoaded").increment(stationMetadataMap.size());
    }

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line == null || line.trim().isEmpty()) {
            context.getCounter("MapperCounters", "EmptyLinesSkipped").increment(1);
            return; // Skip empty lines
        }
        context.getCounter("MapperCounters", "NonEmptyLinesRead").increment(1);

        String[] parts = line.split(POLLUTION_DELIMITER, -1);

        if (!headerProcessed) {
            // This is the header row of the pollutant file
            if (parts.length > 1) {
                for (int i = 1; i < parts.length; i++) { // PĘTLA NAGŁÓWKA
                    String rawColumnHeader = parts[i].trim();
                    Matcher matcher = stationIdPattern.matcher(rawColumnHeader);
                    if (matcher.matches()) {
                        String cleanStationID = matcher.group(1);
                        columnIndexToCleanStationID.put(i, cleanStationID);
                        context.getCounter("MapperCounters", "TotalHeaderStationIDsExtracted").increment(1);
                    } else {
                        context.getCounter("MapperCounters", "TotalHeaderColumnsUnmatchedPattern").increment(1);
                        System.err.println("MAPPER WARNING: Header column '" + rawColumnHeader + "' did not match pattern for file " + currentPollutantType);
                    }
                }
            } else {
                // Nagłówek nie ma oczekiwanej liczby części (np. tylko kolumna 'czas')
                System.err.println("MAPPER WARNING: Header line for " + currentPollutantType + " seems malformed or has no station columns: " + line);
                context.getCounter("MapperCounters", "MalformedHeaderLines").increment(1);
            }
            // Ustaw flagę i zlicz nagłówek PO zakończeniu pętli (lub sprawdzeniu)
            headerProcessed = true;
            context.getCounter("MapperCounters", "PollutantHeadersProcessedAttempted").increment(1);
            System.out.println("MAPPER: Processed header for " + currentPollutantType + ". Mapped " + columnIndexToCleanStationID.size() + " clean station IDs.");
            return; // Zakończ przetwarzanie dla linii nagłówka
        }

        // This is a data row
        if (parts.length <= 1) {
            context.getCounter("MapperCounters", "MalformedDataLines").increment(1);
            if (context.getCounter("MapperCounters", "MalformedDataLines").getValue() <= 10) {
                System.err.println("MAPPER MALFORMED DATA LINE (parts.length=" + parts.length + "): " + line);
            }
            return;
        }

        String time = parts[0].trim();

        // Pętla przetwarzająca wartości pomiarów
        for (int i = 1; i < parts.length; i++) {
            context.getCounter("MapperCounters", "MeasurementValuesIterated").increment(1);
            // ... reszta Twojej logiki dla linii danych ...
            // (pobieranie cleanStationID, szukanie stationInfo, context.write)
            String cleanStationID = columnIndexToCleanStationID.get(i);
            if (cleanStationID == null || cleanStationID.isEmpty()) {
                context.getCounter("MapperCounters", "StationIDFromHeaderMappingNullOrEmpty").increment(1);
                // Loguj, która kolumna danych nie ma mapowania z nagłówka
                if (context.getCounter("MapperCounters", "StationIDFromHeaderMappingNullOrEmpty").getValue() <= 10) {
                    System.err.println("MAPPER DATA WARNING: No cleanStationID mapping for data column index " + i + " (header might have been shorter or unmatched). Line: " + line.substring(0, Math.min(line.length(), 100)) + "...");
                }
                continue;
            }

            String measurementValue = parts[i].trim();
            if (measurementValue.isEmpty()) {
                measurementValue = "NA";
            }

            StationInfo stationInfo = stationMetadataMap.get(cleanStationID);
            if (stationInfo != null) {
                context.getCounter("MapperCounters", "StationInfoFound").increment(1);
                String outputKey = cleanStationID + KEY_VALUE_SEPARATOR + time;
                String outputValue = currentPollutantType + POLLUTANT_VALUE_SEPARATOR + measurementValue +
                        PAYLOAD_SEPARATOR + stationInfo.lat +
                        PAYLOAD_SEPARATOR + stationInfo.lon +
                        PAYLOAD_SEPARATOR + stationInfo.state +
                        PAYLOAD_SEPARATOR + stationInfo.city;
                context.write(new Text(outputKey), new Text(outputValue));
                context.getCounter("MapperCounters", "MeasurementsEmitted").increment(1);
            } else {
                context.getCounter("MapperCounters", "StationInfoNotFoundTotal").increment(1);
            }
        }
    }
}
