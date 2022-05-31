package com.danielstawiarski.sparkexercise;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.sparkproject.jetty.server.handler.ContextHandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        List<String> logs = new ArrayList<>();
        logs.add("WARN: Tuesday 4 September 0405");
        logs.add("ERROR: Tuesday 4 September 0408");
        logs.add("FATAL: Wednesday 5 September 1632");
        logs.add("ERROR: Friday 7 September 1854");
        logs.add("WARN: Saturday 8 September 1942");

        setLogsToWarnLevel();

        SparkConf conf = new SparkConf().setAppName("startingSpark")
                .setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        //Map side reduce is more efficient than simply count() call
        //Long count = sqrtRdd.map(value -> 1L).reduce(Long::sum);

        //Calculate number of specific level logs using reduce by key
//        sparkContext.parallelize(logs)
//                .mapToPair(log -> new Tuple2<>(log.split(":")[0], 1L))
//                .reduceByKey(Long::sum)
//                .foreach(sumTuple -> System.out.println("There is " + sumTuple._2
//                        + " instances of level " + sumTuple._1));

        //How to use flat map to split each row into single expression
        JavaRDD<String> initialRdd = sparkContext.textFile("src/main/resources/subtitles/input.txt");

        JavaRDD<String> words = initialRdd.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
        words.foreach(word -> System.out.println(word));

        sparkContext.close();
    }

    private static void setLogsToWarnLevel() {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger(ContextHandler.class).setLevel(Level.WARN);
    }
}
