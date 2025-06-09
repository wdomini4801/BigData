package org.example.hadoop.WeatherPollutionJoin;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WeatherJoinReducer extends Reducer<Text, Text, NullWritable, Text> {

  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    List<String> pollutionRecords = new ArrayList<>();
    List<String> weatherRecords = new ArrayList<>();

    for (Text val : values) {
      String v = val.toString();
      if (v.startsWith("P|")) {
        pollutionRecords.add(v.substring(2));
      } else if (v.startsWith("W|")) {
        weatherRecords.add(v.substring(2));
      }
    }

    for (String pollution : pollutionRecords) {
      for (String weather : weatherRecords) {
        // Simple join - you can format output as needed
        context.write(NullWritable.get(), new Text(pollution + "," + weather));
      }
    }
  }
}