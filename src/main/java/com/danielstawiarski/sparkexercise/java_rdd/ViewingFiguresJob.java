package com.danielstawiarski.sparkexercise.java_rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.sparkproject.jetty.server.handler.ContextHandler;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * This exercise focus on finding best scored courses based on data related to users and number of
 * chapters per course viewed by user in a particular course.
 * Base assumption is:
 * - if user watched more than 90% of chapters - 10 points for course
 * - if user watched between 50 - 90% of chapters - 4 points for course
 * - if user watched between 25 - 50% of chapters - 2 points for course
 * - less than 25% means that 0 points for course
 * Return the highest scored courses as sorted result.
 */
public class ViewingFiguresJob {

    //TODO Consider performance using DAG diagram. Probably we can limit amount of wide transformations
    // and avoid reading chapters.csv twice to memory using cache/persist.
    @SuppressWarnings("resource")
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger(ContextHandler.class).setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Use true to use hardcoded data identical to that in the PDF guide.
        boolean testMode = false;

        JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc, testMode);
        JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc, testMode);
        JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(sc, testMode);

        JavaPairRDD<Integer, Integer> chapterCountRdd = chapterData.mapToPair(row -> new Tuple2<Integer, Integer>(row._2, 1))
                .reduceByKey(Integer::sum);

        JavaPairRDD<Integer, Integer> courseToScore =
                viewData.distinct()
                        .mapToPair(view -> new Tuple2<>(view._2, view._1))
                        .join(chapterData)
                        .mapToPair(chapterPerUserToCourse -> new Tuple2<>(new Tuple2<>(chapterPerUserToCourse._2._2, chapterPerUserToCourse._2._1), 1))
                        .reduceByKey(Integer::sum)
                        .mapToPair(courseToUserViewCount -> new Tuple2<>(courseToUserViewCount._1._1, courseToUserViewCount._2))
                        .join(chapterCountRdd)
                        .mapToPair(element -> new Tuple2<>(element._1, calculatePoints(element._2._2, element._2._1)))
                        .reduceByKey(Integer::sum);

        courseToScore.join(titlesData)
                .mapToPair(element -> new Tuple2<>(element._2._1, element._2._2))
                .sortByKey(false).take(40).forEach(element -> System.out.println(element));

        sc.close();
    }

    private static Integer calculatePoints(Integer totalChapters, Integer watchedChapters) {
        int courseCoveredPercentage = watchedChapters * 100 / totalChapters;
        if (courseCoveredPercentage > 90) return 10;
        if (courseCoveredPercentage > 50 && courseCoveredPercentage < 90) return 4;
        if (courseCoveredPercentage > 25 && courseCoveredPercentage < 50) return 2;
        return 0;
    }

    private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {

        if (testMode) {
            // (chapterId, title)
            List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
            rawTitles.add(new Tuple2<>(1, "How to find a better job"));
            rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
            rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
            return sc.parallelizePairs(rawTitles);
        }
        return sc.textFile("src/main/resources/viewing figures/titles.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] cols = commaSeparatedLine.split(",");
                    return new Tuple2<Integer, String>(new Integer(cols[0]), cols[1]);
                });
    }

    private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {

        if (testMode) {
            // (chapterId, (courseId, courseTitle))
            List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
            rawChapterData.add(new Tuple2<>(96, 1));
            rawChapterData.add(new Tuple2<>(97, 1));
            rawChapterData.add(new Tuple2<>(98, 1));
            rawChapterData.add(new Tuple2<>(99, 2));
            rawChapterData.add(new Tuple2<>(100, 3));
            rawChapterData.add(new Tuple2<>(101, 3));
            rawChapterData.add(new Tuple2<>(102, 3));
            rawChapterData.add(new Tuple2<>(103, 3));
            rawChapterData.add(new Tuple2<>(104, 3));
            rawChapterData.add(new Tuple2<>(105, 3));
            rawChapterData.add(new Tuple2<>(106, 3));
            rawChapterData.add(new Tuple2<>(107, 3));
            rawChapterData.add(new Tuple2<>(108, 3));
            rawChapterData.add(new Tuple2<>(109, 3));
            return sc.parallelizePairs(rawChapterData);
        }

        return sc.textFile("src/main/resources/viewing figures/chapters.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] cols = commaSeparatedLine.split(",");
                    return new Tuple2<Integer, Integer>(Integer.valueOf(cols[0]), Integer.valueOf(cols[1]));
                });
    }

    private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc, boolean testMode) {

        if (testMode) {
            // Chapter views - (userId, chapterId)
            List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
            rawViewData.add(new Tuple2<>(14, 96));
            rawViewData.add(new Tuple2<>(14, 97));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(14, 99));
            rawViewData.add(new Tuple2<>(13, 100));
            return sc.parallelizePairs(rawViewData);
        }

        return sc.textFile("src/main/resources/viewing figures/views-*.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] columns = commaSeparatedLine.split(",");
                    return new Tuple2<Integer, Integer>(new Integer(columns[0]), new Integer(columns[1]));
                });
    }
}
