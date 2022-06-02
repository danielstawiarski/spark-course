package com.danielstawiarski.sparkexercise.spark_sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.sparkproject.jetty.server.handler.ContextHandler;

import static org.apache.spark.sql.functions.*;

/**
 * This exercise serves to generate report with average and standard deviation per year using pivot table
 * for big data file with students results
 */
public class ExamsResultsReportExercise {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger(ContextHandler.class).setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("sqlPlay").master("local[*]").getOrCreate();

        Dataset<Row> dataSet = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");

        dataSet = dataSet.groupBy("subject").pivot("year").agg(
                round(avg(col("score")), 2).alias("score_average"),
                round(stddev(col("score")), 2).alias("score_standard_deviation")
        );

        dataSet.show();
    }
}
