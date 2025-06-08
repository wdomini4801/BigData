package org.example.hadoop.Aggregate2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PollutantAggregate2Driver {
  public static int runJob(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 3) {
      System.err
          .println("Usage: PollutantAggregate2Driver <pollutants_input_dir> <metadata_file_hdfs_path> <output_dir>");
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
    Path intermediatePath = new Path(outputDir + "_intermediate");
    if (fs.exists(intermediatePath)) {
      fs.delete(intermediatePath, true);
    }

    // Pass metadata file path to the job configuration
    conf.set("metadata.file.path", metadataFileHdfsPath);

    // DEBUG: Set maximum records to process (comment out for production)
    conf.setLong("debug.max.records", 300);
    conf.set("mapreduce.reduce.memory.mb", "4096");
    conf.set("mapreduce.reduce.java.opts", "-Xmx3072m");

    // Job 1: Process and parse all input files
    Job job1 = Job.getInstance(conf, "pollutant data processing");
    job1.setJarByClass(PollutantAggregate2Driver.class);
    job1.setMapperClass(AirQualityMapper.class);
    job1.setReducerClass(AirQualityReducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);

    // Add both pollutants directory and metadata file as inputs
    FileInputFormat.addInputPath(job1, new Path(pollutantsInputDir));
    FileInputFormat.addInputPath(job1, new Path(metadataFileHdfsPath));
    FileOutputFormat.setOutputPath(job1, new Path(outputDir + "_intermediate"));

    if (!job1.waitForCompletion(true)) {
      return 1;
    }

    // Job 2: Final aggregation
    Job job2 = Job.getInstance(conf, "pollutant final aggregation");
    job2.setJarByClass(PollutantAggregate2Driver.class);
    job2.setMapperClass(KeyValueLineMapper.class);
    job2.setReducerClass(FinalAggregationReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job2, new Path(outputDir + "_intermediate"));
    FileOutputFormat.setOutputPath(job2, new Path(outputDir));

    return job2.waitForCompletion(true) ? 0 : 1;
  }
}
