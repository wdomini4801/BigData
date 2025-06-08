package org.example.hadoop.Aggregate2;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.example.hadoop.Aggregate2.Models.MeasurementData;

public class PollutantAggregate2Driver {

  public static int runJob(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 3) {
      System.err.println("Usage: PollutantJoinDriver <pollutants_input_dir> <metadata_file_hdfs_path> <output_dir>");
      System.exit(2);
    }

    String pollutantsInputDir = otherArgs[0];
    String metadataFileHdfsPath = otherArgs[1];
    String outputDir = otherArgs[2];

    FileSystem fs = FileSystem.get(conf);
    Path outputPath = new Path(outputDir);
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true);
    }

    Job job = Job.getInstance(conf, "Join Pollutants with Station Metadata");
    job.setJarByClass(PollutantAggregate2Driver.class);

    job.setMapperClass(MeasurementMapper.class);
    job.setReducerClass(JoinReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(MeasurementData.class);

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(pollutantsInputDir));
    FileOutputFormat.setOutputPath(job, outputPath);

    // Add metadata to distributed cache
    job.addCacheFile(new URI(metadataFileHdfsPath + "#station_metadata.csv")); // Local symlink in mappers

    return job.waitForCompletion(true) ? 0 : 1;

  }
}
