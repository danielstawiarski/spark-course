package com.danielstawiarski.sparkexercise.java_rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.sparkproject.jetty.server.handler.ContextHandler;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Scanner;

/**
 * This job serves to finding 10 relevant tags for course base on chapters related provided input (input.txt file).
 */
public class SearchForCourseTagsJob {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger(ContextHandler.class).setLevel(Level.WARN);


        SparkConf conf = new SparkConf().setAppName("startingSpark")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> subtitles = sc.textFile("src/main/resources/subtitles/input.txt");

        subtitles
                .filter(line -> !line.contains("-->") && !line.isEmpty())
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .map(String::toLowerCase)
                .map(SearchForCourseTagsJob::cleanWord)
                .filter(SearchForCourseTagsJob::hasSense)
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .take(10)
                .forEach(tuple -> System.out.println("Tag \"" + tuple._2 + "\" appears " + tuple._1));

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

    }

    private static boolean hasSense(String word) {
        return Util.isNotBoring(word) && !isNumber(word) && !word.contains("'");
    }

    private static String cleanWord(String word) {
        String withoutDots = word.replaceAll("\\.", "");
        String withoutCommas = withoutDots.replaceAll(",", "");
        return withoutCommas;
    }

    private static boolean isNumber(String word) {
        try {
            Integer.parseInt(word);
            return true;
        } catch (NumberFormatException nfe) {
            return false;
        }
    }
}
