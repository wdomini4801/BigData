package org.example.hadoop.PollutantAggregate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.net.URI;

public class PollutantAggregateDriver {

    public static int runJob(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage: PollutantJoiningDriver <pollutants_input_dir> <metadata_file_hdfs_path> <output_dir>");
            System.exit(2);
        }

        String pollutantsInputDir = otherArgs[0];
        String metadataFileHdfsPath = otherArgs[1]; // e.g., /user/your_user/metadata/stations-metadata.csv
        String outputDir = otherArgs[2];

        Job job = Job.getInstance(conf, "Pollutant Data Join");

        job.setJarByClass(PollutantAggregateDriver.class);
        job.setMapperClass(PollutantJoinMapper.class);
        // Combiner is not suitable here because we need all pollutant types for a key in the reducer.
        job.setReducerClass(PollutantAggregateReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(3);

        FileInputFormat.addInputPath(job, new Path(pollutantsInputDir));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        // Add metadata file to DistributedCache
        job.addCacheFile(new URI(metadataFileHdfsPath));


        // Ensure output directory doesn't exist
        org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
        if (fs.exists(new Path(outputDir))) {
            fs.delete(new Path(outputDir), true); // true for recursive
            System.out.println("Deleted existing output directory: " + outputDir);
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
