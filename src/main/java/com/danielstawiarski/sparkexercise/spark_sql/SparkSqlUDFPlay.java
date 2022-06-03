package com.danielstawiarski.sparkexercise.spark_sql;

import static org.apache.spark.sql.functions.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.sparkproject.jetty.server.handler.ContextHandler;

public class SparkSqlUDFPlay {
    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger(ContextHandler.class).setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("sqlPlay").master("local[*]").getOrCreate();

        spark.udf().register("hasPassed", (String grade, String subject) ->
        {
            if (subject.equals("Biology")) {
                return grade.startsWith("A");
            }
            return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
        }, DataTypes.BooleanType);

        Dataset<Row> dataSet = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");

        dataSet = dataSet.withColumn("passed", callUDF("hasPassed", col("grade"), col("subject")));

        dataSet.show();

    }
}
