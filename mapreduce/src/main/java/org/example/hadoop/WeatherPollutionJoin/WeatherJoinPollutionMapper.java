package org.example.hadoop.WeatherPollutionJoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeatherJoinPollutionMapper extends Mapper<LongWritable, Text, Text, Text> {

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();

    try {
      String[] fields = line.split(",");

      if (fields.length < 3 || fields[0].length() < 13)
        return;

      String time = fields[0].substring(0, 13);  // yyyy-MM-ddTHH
      String stationId = fields[1];
      String compositeKey = time + "|" + stationId;

      context.write(new Text(compositeKey), new Text("P|" + line));
    } catch (Exception e) {
      throw new Error("Error processing pollution line: " + line);
    }
  }
}
