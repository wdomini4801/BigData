package org.example.hadoop.Aggregate2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class KeyValueLineMapper extends Mapper<LongWritable, Text, Text, Text> {
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String[] parts = value.toString().split("\t", 2); // tab-separated
    if (parts.length == 2) {
      context.write(new Text(parts[0]), new Text(parts[1]));
    }
  }
}