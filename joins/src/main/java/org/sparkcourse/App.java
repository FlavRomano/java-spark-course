package org.sparkcourse;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.tinylog.Logger;
import scala.Tuple2;

public class App
{
    private static void firstExercise(JavaSparkContext sc) {
        Logger.info("FIRST EXERCISE START");

        JavaRDD<String> chapterCourseMapping = sc.textFile("src/main/resources/chapters.csv");
        JavaPairRDD<String, Long> javaPairRDD = chapterCourseMapping.mapToPair(
                row -> new Tuple2<>( row.split(",")[1], 1L)
        ).sortByKey();

        JavaPairRDD<Long, String> reducedRDD = javaPairRDD.reduceByKey(
                        (l1, l2) -> l1+l2
                ).mapToPair(t -> new Tuple2<>( t._2, t._1 ))
                .sortByKey(false);

        Logger.info("{}", reducedRDD.collect());

        Logger.info("FIRST EXERCISE END");
    }

    private static void secondExercise(JavaSparkContext sc) {
        Logger.info("SECOND EXERCISE START");

        JavaRDD<String> viewsRDD = sc.textFile("src/main/resources/views-*.csv");

        // chapterId, courseId
        JavaPairRDD<Long, Long> chaptersRDD = sc.textFile("src/main/resources/chapters.csv")
                .mapToPair(row -> new Tuple2<>(Long.valueOf(row.split(",")[0]), Long.valueOf(row.split(",")[1])));

        // chapterId, userId, courseId
        JavaPairRDD<Long, Tuple2<Long, Long>> viewPair = viewsRDD
                .mapToPair(row ->
                        new Tuple2<>( Long.valueOf(row.split(",")[1]), Long.valueOf(row.split(",")[0]))
                )
                .distinct()
                .join(chaptersRDD);

        // view(userId, courseId)
        JavaPairRDD<Long, Long> userIdCourseIdRDD = viewPair.mapToPair(t -> t._2);

        // userId, courseId, #chaptersSeen
        JavaPairRDD<Tuple2<Long, Long>, Long> countRDD = userIdCourseIdRDD
                .mapToPair(t -> new Tuple2<>(t, 1L)) // <<userId, courseId>, 1>
                .reduceByKey((l1, l2) -> l1+l2);

        // courseId, #chaptersSeen
        JavaPairRDD<Long, Long> courseIdFirstCountRDD = countRDD
                .mapToPair(t -> new Tuple2<>(t._1._1, t._2));

        // courseId, #chapters
        JavaPairRDD<Long, Long> courseCountRDD = chaptersRDD
                .mapToPair(t -> new Tuple2<>(t._2, 1L))
                .reduceByKey((l1, l2) -> l1+l2);

        JavaPairRDD<Long, Long> courseScoreRDD = courseCountRDD
                .join(courseIdFirstCountRDD)
                .mapToPair(t -> {
                    long courseId = t._1;
                    long totChapters = t._2._1;
                    long userSeenChapter = t._2._2;

                    double percentage = (double) (userSeenChapter * 100) / totChapters;

                    long score = 0;
                    if (percentage > 90)
                        score = 10;
                    else if (percentage > 50 && percentage < 90)
                        score = 4;
                    else if (percentage > 25 && percentage < 50)
                        score = 2;

                    return new Tuple2<>(courseId, score);
                })
                .reduceByKey((l1, l2) -> l1+l2);

        JavaPairRDD<Long, String> courseTitlesRDD = sc.textFile("src/main/resources/titles.csv")
                .mapToPair(row -> new Tuple2<>(Long.valueOf(row.split(",")[0]), row.split(",")[1]));

        JavaPairRDD<String, Long> sortedScoreboardRDD = courseScoreRDD
                .join(courseTitlesRDD)
                .mapToPair(t -> new Tuple2<>(t._2._1, t._2._2))
                .sortByKey(false)
                .mapToPair(t -> new Tuple2<>(t._2, t._1));

        Logger.info("{}", sortedScoreboardRDD.collect());

        Logger.info("SECOND EXERCISE END");
    }

    public static void main( String[] args )
    {
        Logger.info("Start spark app");

        SparkConf sparkConf = new SparkConf()
                .setAppName("Spark Course App")
                .setMaster("local[*]"); // use all the available cores on the machine


        try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            firstExercise(sc);
            secondExercise(sc);
        } catch (Exception e) {
            Logger.error(e);
        }
    }
}
