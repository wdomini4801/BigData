package org.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class AggregateMetadataJob {
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

        // Load measurements - Process each pollutant file separately
        List<String> pollutants = Arrays.asList("PM10", "PM25", "SO2", "NO2", "C6H6");
        Dataset<Row> allMeasurements = null;

        for (String pollutant : pollutants) {
            String filePath = measurementPath + pollutant + "_1g_joint_2017-2023.csv";

            Dataset<Row> df = spark.read()
                    .option("header", "true")
                    .option("inferSchema", "false") // Keep as strings initially
                    .csv(filePath);

            List<String> allColumns = Arrays.asList(df.columns());

            // Filter to find the selected stations subset
            List<String> selectedColumns = allColumns.stream()
                    .filter(c -> c.equals("Time") || RUN_ONLY.stream().anyMatch(station -> c.startsWith(station)))
                    .collect(Collectors.toList());

            // Limit the dataframe to those found columns
            Column[] columnSelections = selectedColumns.stream()
                    .map(colName -> expr("`" + colName + "`")) // escape to handle "-" and '.'
                    .toArray(Column[]::new);
            df = df.select(columnSelections);

            // Build stack expression to unpivot columns into (header, value)
            StringBuilder expr = new StringBuilder();
            expr.append("stack(").append(selectedColumns.size() - 1);
            for (String colName : selectedColumns) {
                if (!colName.equals("Time")) {
                    // Properly escape column names containing dots and hyphens
                    expr.append(", '").append(colName).append("', `").append(colName).append("`");
                }
            }
            expr.append(") as (header, value)");

            Dataset<Row> longDF = df.selectExpr("Time", expr.toString())
                    .withColumn("value", col("value").cast(DataTypes.DoubleType))
                    .filter(col("value").isNotNull()); // Remove null values

            // Extract StationId from header (e.g., "DsJelGorOgin-C6H6-1g" ->
            // "DsJelGorOgin")
            Dataset<Row> parsedDF = longDF
                    .withColumn("StationId", regexp_extract(col("header"), "^([^-]+)", 1))
                    .withColumn("pollutant", lit(pollutant))
                    .drop("header")
                    .filter(col("StationId").isNotNull().and(col("StationId").notEqual("")));

            if (allMeasurements == null) {
                allMeasurements = parsedDF;
            } else {
                allMeasurements = allMeasurements.unionByName(parsedDF);
            }
        }

        // Filter for RUN_ONLY stations
        Column stationFilter = col("StationId").isin(RUN_ONLY.toArray());
        Dataset<Row> filteredDF = allMeasurements.filter(stationFilter);

        // Pivot back to wide format by pollutant
        Dataset<Row> pivotedDF = filteredDF.groupBy("Time", "StationId")
                .pivot("pollutant", new ArrayList<Object>(pollutants))
                .agg(first("value"));

        // Load station metadata
        Dataset<Row> metadata = spark.read()
                .option("header", "true")
                .option("delimiter", ";")
                .csv(metadataPath)
                .select(
                        col("StationID").alias("metadataStationId"),
                        col("InternationalStationID"),
                        col("lat").alias("Latitude"),
                        col("long").alias("Longitude"));

        // Join metadata with pivoted measurements
        spark.udf().register("partialMatch",
                (String pivotStation, String metaStation) -> metaStation != null && metaStation.contains(pivotStation),
                DataTypes.BooleanType);

        Dataset<Row> joinedDF = pivotedDF.join(metadata,
                col("metadataStationId").contains(col("StationId")),
                "inner");

        // Final column order
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

        // Show sample data and count
        System.out.println("Final dataset count: " + finalDF.count());
        finalDF.show(20);

        // Write output
        finalDF.coalesce(1)
                .write()
                .mode("overwrite")
                .option("header", true)
                .csv("hdfs:///data/output/aggregate-result");

        spark.stop();
    }
}
