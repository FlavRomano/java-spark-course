package org.sparkcourse;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.tinylog.Logger;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Locale;

public class App {

    public static void main( String[] args ) {
        Logger.info("Start spark app");

        SparkConf sparkConf = new SparkConf()
                .setAppName("Spark Course App")
                .setMaster("local[*]"); // use all the available cores on the machine
        try (var sc = new JavaSparkContext(sparkConf)) {
            JavaRDD<String> fileRdd = sc.textFile("src/main/resources/Barry Lyndon.txt");

            JavaRDD<String> filtered = fileRdd.filter(
                    line -> !line.contains(":") && StringUtils.isNotBlank(line)
            );

            JavaRDD<String> splitted = filtered.flatMap(line -> {
                String cleanedLine = line.replaceAll("\\p{Punct}", "");
                return Arrays.asList(cleanedLine.toLowerCase(Locale.ROOT).split(" ")).iterator();
            });

            JavaPairRDD<String, Long> reducedRDD = splitted
                    .filter(word -> Util.isNotCommon(word) && !StringUtils.isNumeric(word) && StringUtils.isNotBlank(word))
                    .mapToPair(word -> new Tuple2<>(word, 1L))
                    .reduceByKey((a, b) -> a + b);

            JavaPairRDD<Long, String> switched = reducedRDD
                    .mapToPair(t -> new Tuple2<Long, String>(t._2, t._1))
                    .sortByKey(false);

            switched.foreach(Logger::info);
        } catch (RuntimeException e) {
            Logger.error(e);
        }
    }
}
