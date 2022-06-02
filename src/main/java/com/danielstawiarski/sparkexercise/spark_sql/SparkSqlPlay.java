package com.danielstawiarski.sparkexercise.spark_sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.sparkproject.jetty.server.handler.ContextHandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * Test class.
 */
public class SparkSqlPlay {
    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger(ContextHandler.class).setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("sqlPlay").master("local[*]").getOrCreate();

//        System.out.println("There are: " + dataSet.count() + " of records");
        //Filters
//        Dataset<Row> modernArtWitYearGreaterThanRows = dataSet.filter("subject = 'Modern Art' AND year >= 2007");

//        Dataset<Row> modernArtWitYearGreaterThanRows = dataSet.filter((FilterFunction<Row>) row -> row.getAs("subject").equals("Modern Art") &&
//                Integer.parseInt(row.getAs("year")) >= 2007);

//        Dataset<Row> modernArtWitYearGreaterThanRows = dataSet.filter(col("subject").equalTo("Modern Art")
//                .and(col("year").geq("2007")));
//
//        modernArtWitYearGreaterThanRows.show();

        //Create temp view and get unique years ordered descending
//        dataSet.createOrReplaceTempView("students");
//        Dataset<Row> years = spark.sql("select distinct(year) from students order by year desc");
//        years.show();

//        Dataset<Row> testDataSet = createTestDataSet(spark);

        Dataset<Row> testDataSet = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
//        testDataSet.createOrReplaceTempView("logs_table");
//        Dataset<Row> results = spark.sql("select level, date_format(datetime, 'MMMM') as month,count(1) as total from logs_table group by level, month " +
//                "order by cast(first(date_format(datetime, 'M')) as int), level");

        Dataset<Row> results = testDataSet.select(col("level"),
                date_format(col("datetime"), "MMMM").alias("month"),
                date_format(col("datetime"), "M").alias("monthnumber").cast(DataTypes.IntegerType));
//        results = results.groupBy(col("month"), col("level"), col("monthnumber")).count();
//        results = results.orderBy(col("monthnumber"), col("level"));
//        results = results.drop(col("monthnumber"));
        List<Object> months = Arrays.asList("January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December");
        results = results.groupBy(col("level")).pivot(col("month"), months).count().na().fill(0);


        results.show(100);
        spark.close();
    }

    private static Dataset<Row> createTestDataSet(SparkSession spark) {
        List<Row> testRows = new ArrayList<>();

        testRows.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
        testRows.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
        testRows.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
        testRows.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
        testRows.add(RowFactory.create("FATAL", "2015-4-21 19:23:20"));

        StructField[] fields = new StructField[]{
                StructField.apply("level", DataTypes.StringType, false, Metadata.empty()),
                StructField.apply("datetime", DataTypes.StringType, false, Metadata.empty())
        };
        StructType schema = new StructType(fields);
        Dataset<Row> testDataSet = spark.createDataFrame(testRows, schema);
        return testDataSet;
    }
}
