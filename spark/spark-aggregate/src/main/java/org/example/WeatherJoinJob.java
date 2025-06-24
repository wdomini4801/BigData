package org.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.*;
import java.util.*;
import static org.apache.spark.sql.functions.*;

public class WeatherJoinJob {
    public static void main(String[] args) {
        String pollutionInputPath = "hdfs:///data/output/aggregate-result/";
        String weatherInputPath = "hdfs:///data/openmeteo/2017/";
        String outputPath = "hdfs:///data/output/aggregate-weather-joined/";

        SparkSession spark = SparkSession.builder()
                .appName("Weather Join Job")
                .master("yarn")
                .getOrCreate();

        Set<String> RUN_ONLY = new HashSet<>(Arrays.asList(
                "DsJelGorOgin", "DsWrocWybCon", "KpBydPlPozna", "KpBydWarszaw", "LdLodzGdansk"));

        // Load Pollution data (output of previous job)
        Dataset<Row> pollutionDF = spark.read()
                .option("header", true)
                .csv(pollutionInputPath)
                .withColumn("Time", substring(col("Time"), 0, 13)) // Ensure hour precision
                .filter(col("StationId").isin(RUN_ONLY.toArray()));

        // Load Weather data
        // Read as text first, then process each file
        Dataset<Row> weatherRaw = spark.read()
                .textFile(weatherInputPath + "/*.csv")
                .withColumn("input_filename", input_file_name())
                .withColumn("StationId", regexp_extract(col("input_filename"), "openmeteo_(.*?)_\\d{4}\\.csv", 1))
                .filter(col("StationId").isin(RUN_ONLY.toArray()));

        // Split lines and get row numbers to skip first 2 lines per file
        Dataset<Row> weatherLines = weatherRaw
                .withColumn("row_id", row_number().over(
                    Window.partitionBy("StationId", "input_filename").orderBy(lit(1))))
                .filter(col("row_id").gt(3))  // Skip first 3 lines (2 lines for request metadata and one empty)
                .filter(not(col("value").startsWith("time")))  // Extra safety to remove any header lines
                .withColumn("split_data", split(col("value"), ","));

        // Create properly typed columns
        Dataset<Row> weatherProcessed = weatherLines
                .filter(size(col("split_data")).equalTo(8))  // Ensure complete rows
                .select(
                    col("StationId"),
                    col("split_data").getItem(0).as("time"),
                    col("split_data").getItem(1).cast("double").as("temperature_2m"),
                    col("split_data").getItem(2).cast("double").as("relative_humidity_2m"),
                    col("split_data").getItem(3).cast("double").as("precipitation"),
                    col("split_data").getItem(4).cast("double").as("surface_pressure"),
                    col("split_data").getItem(5).cast("double").as("wind_speed_10m"),
                    col("split_data").getItem(6).cast("double").as("wind_direction_10m"),
                    col("split_data").getItem(7).cast("double").as("direct_radiation")
                )
                .withColumn("Time", substring(col("time"), 0, 13));  // Keep both time and Time columns

        System.out.println("Weather data sample:");
        weatherProcessed.show(5);

        // Join pollution with weather
        Dataset<Row> joinedDF = pollutionDF.join(weatherProcessed,
                pollutionDF.col("Time").equalTo(weatherProcessed.col("Time"))
                        .and(pollutionDF.col("StationId").equalTo(weatherProcessed.col("StationId"))),
                "inner")
                .drop(weatherProcessed.col("Time"))
                .drop(weatherProcessed.col("StationId"));  // Remove duplicate StationId column

        System.out.println("Joined dataset count: " + joinedDF.count());
        joinedDF.show(20);

        joinedDF.coalesce(1)
                .write()
                .mode("overwrite")
                .option("header", true)
                .csv(outputPath);

        spark.stop();
    }
}
