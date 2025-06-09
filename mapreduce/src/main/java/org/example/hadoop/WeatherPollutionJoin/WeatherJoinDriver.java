package org.example.hadoop.WeatherPollutionJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WeatherJoinDriver {
    public static int runJob(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage: WeatherJoinDriver <pollution_flat_input> <weather_dir> <output_dir>");
            System.exit(2);
        }

        Path pollutionInput = new Path(otherArgs[0]);
        Path weatherInput = new Path(otherArgs[1]);
        Path output = new Path(otherArgs[2]);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }

        Job job = Job.getInstance(conf, "Join Weather with Pollution");
        job.setJarByClass(WeatherJoinDriver.class);

        MultipleInputs.addInputPath(job, pollutionInput, TextInputFormat.class, WeatherJoinPollutionMapper.class);
        MultipleInputs.addInputPath(job, weatherInput, TextInputFormat.class, WeatherJoinWeatherMapper.class);

        job.setReducerClass(WeatherJoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
