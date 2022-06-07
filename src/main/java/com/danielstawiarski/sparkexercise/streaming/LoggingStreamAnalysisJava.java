package com.danielstawiarski.sparkexercise.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.sparkproject.jetty.server.handler.ContextHandler;
import scala.Tuple2;

/**
 * Count logs by level.
 * Batch each 3 seconds.
 * Windows is 2 minutes (to aggregate many batch results together)
 */
public class LoggingStreamAnalysisJava {
    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger(ContextHandler.class).setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("streamingSpark").setMaster("local[*]");
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(3));

        JavaReceiverInputDStream<String> lines = sc.socketTextStream("localhost", 8989);
        JavaPairDStream<String, Long> levelCount = lines
                .mapToPair(LoggingStreamAnalysisJava::splitLog)
                .reduceByKeyAndWindow(Long::sum, Durations.minutes(2));

        levelCount.print();

        sc.start();
        sc.awaitTermination();
    }

    private static Tuple2<String, Long> splitLog(String logLine) {
        String[] columns = logLine.split(",");
        return new Tuple2<>(columns[0], 1L);
    }
}
