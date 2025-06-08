package org.example;

import org.example.hadoop.Aggregate2.PollutantAggregate2Driver;
import org.example.hadoop.PollutantAggregate.PollutantAggregateDriver;
import org.example.hadoop.WeatherPollutionJoin.WeatherJoinDriver;

import java.util.Arrays;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: App <jobName> [job args]");
            System.exit(1);
        }

        String jobName = args[0];
        String[] jobArgs = Arrays.copyOfRange(args, 1, args.length);

        int exitCode = -1;

        switch (jobName.toLowerCase()) {
            case "pollutant-aggregate":
                exitCode = PollutantAggregateDriver.runJob(jobArgs);
                break;
            case "pollutant-aggregate-2":
                exitCode = PollutantAggregate2Driver.runJob(jobArgs);
                break;
            case "join-weather":
                exitCode = WeatherJoinDriver.runJob(jobArgs);
                break;
            default:
                System.err.println("Unknown job: " + jobName);
                System.exit(2);
        }

        System.exit(exitCode);
    }
}
