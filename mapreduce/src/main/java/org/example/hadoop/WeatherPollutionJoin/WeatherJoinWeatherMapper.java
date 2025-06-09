package org.example.hadoop.WeatherPollutionJoin;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WeatherJoinWeatherMapper extends Mapper<LongWritable, Text, Text, Text> {
  private String stationId = "";

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    FileSplit fileSplit = getFileSplit(context);
    String filename = fileSplit.getPath().getName();
    Pattern pattern = Pattern.compile("openmeteo_(.*)_\\d{4}\\.csv");
    Matcher matcher = pattern.matcher(filename);
    if (matcher.find()) {
      stationId = matcher.group(1);
    }
  }

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();
    if (line.startsWith("latitude") ||
        line.matches("^\\d+\\.\\d+,.+") || // lines starting with a float number (latitude)
        line.startsWith("time") ||
        stationId.isEmpty()) {
      return;
    }

    String[] fields = line.split(",");
    if (fields.length < 2)
      return;

    try {
      String time = fields[0].substring(0, 13); // Extract hour precision
      String compositeKey = time + "|" + stationId;
      context.write(new Text(compositeKey), new Text("W|" + value.toString()));
    }  catch (Exception e) {
      throw new Error("Error processing weather line: " + line);
    }
    
  }

  private FileSplit getFileSplit(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException {
    InputSplit split = context.getInputSplit();
    if (split instanceof FileSplit) {
      return (FileSplit) split;
    }

    try {
      if (split.getClass().getName().equals("org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit")) {
        Method getSplit = split.getClass().getDeclaredMethod("getInputSplit");
        getSplit.setAccessible(true);
        return (FileSplit) getSplit.invoke(split);
      }
    } catch (Exception e) {
      throw new IOException("Unable to unwrap InputSplit", e);
    }

    throw new IOException("Unsupported InputSplit type: " + split.getClass().getName());
  }

}
