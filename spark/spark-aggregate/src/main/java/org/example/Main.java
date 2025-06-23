package org.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.util.*;
import java.util.regex.*;

import static org.apache.spark.sql.functions.*;

public class Main {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Spark Aggregate Conversion")
                .master("yarn") // or just leave it default on cluster
                .getOrCreate();

        // === PARAMETERS ===
        String measurementPath = "hdfs:///data/kaggle/joint_data_2017-2023/";
        String metadataPath = "hdfs:///data/kaggle/stations_metadata.csv";
        Set<String> RUN_ONLY = new HashSet<>(Arrays.asList(
                "DsJelGorOgin", "DsWrocWybCon", "KpBydPlPozna", "KpBydWarszaw", "LdLodzGdansk"));

        // === 1. Load measurements ===
        Dataset<Row> measurements = spark.read()
                .option("header", "true")
                .option("inferSchema", "false")
                .csv(measurementPath);

        // === 2. Melt wide data into long format ===
        // Convert all columns except timestamp into rows

        List<String> columns = new ArrayList<>(Arrays.asList(measurements.columns()));
        columns.remove("Time");

        // Use stack to unpivot
        StringBuilder expr = new StringBuilder();
        expr.append("stack(").append(columns.size());
        for (String col : columns) {
            expr.append(", '").append(col).append("', ").append("`").append(col).append("`");
        }
        expr.append(") as (header, value)");

        Dataset<Row> longDF = measurements.selectExpr("Time", expr.toString())
            .withColumn("value", col("value").cast(DataTypes.DoubleType));

        // === 3. Extract StationId and pollutant from header ===
        // Example: DsBoleslaMOB-PM10-1g → StationId: DsBoleslaMOB, pollutant: PM10

        Dataset<Row> parsedDF = longDF
                .withColumn("StationId", regexp_extract(col("header"), "^([^-]+)", 1))
                .withColumn("pollutant", regexp_replace(
                        regexp_extract(col("header"), "-([A-Za-z0-9]+)", 1),
                        "\\d+g$", ""))
                .drop("header");

        // === 4. Filter by RUN_ONLY stations ===

        Column stationFilter = col("StationId").isin(RUN_ONLY.toArray());
        Dataset<Row> filteredDF = parsedDF.filter(stationFilter);

        // === 5. Pivot to wide format again ===

        Dataset<Row> pivotedDF = filteredDF.groupBy("Time", "StationId")
                .pivot("pollutant", Arrays.asList("PM10", "PM25", "SO2", "NO2", "C6H6"))
                .agg(first("value"));

        // === 6. Load station metadata ===

        Dataset<Row> metadata = spark.read()
                .option("header", "true")
                .option("delimiter", ";")
                .csv(metadataPath)
                .select(
                        col("StationID").alias("metadataStationId"),
                        col("InternationalStationID"),
                        col("lat").alias("Latitude"),
                        col("long").alias("Longitude"));

        // === 7. Join metadata with pivoted measurements ===

        // Note: partial station id match as in your reducer (contains logic)

        // Broadcast join using UDF for "contains"
        spark.udf().register("partialMatch",
                (String pivotStation, String metaStation) -> metaStation.contains(pivotStation),
                DataTypes.BooleanType);

        Dataset<Row> joinedDF = pivotedDF
                .crossJoin(metadata)
                .filter(expr("partialMatch(StationId, metadataStationId)"))
                .dropDuplicates("Time", "StationId"); // ensure 1:1 after crossJoin

        // === 8. Final column order ===

        Dataset<Row> finalDF = joinedDF.select(
                col("Time"),
                col("StationId"),
                col("InternationalStationId"),
                col("Latitude"),
                col("Longitude"),
                col("PM10"),
                col("PM25"),
                col("SO2"),
                col("NO2"),
                col("C6H6"));

        // === 9. Show & write output ===

        finalDF.show();
        finalDF.coalesce(1)
                .write()
                .option("header", true)
                .csv("hdfs:///data/output/aggregate-result");

        spark.stop();
    }
}
