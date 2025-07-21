package com.sparkcourse.caching;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.tinylog.Logger;
import scala.Tuple2;


public final class SparkPi {
    public static void main(String[] args) {
        Logger.info("Start spark app");

        SparkConf sparkConf = new SparkConf()
                .setAppName("Spark Course App")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaPairRDD<String, Integer> paroleComuni = sc.textFile("demo/src/main/resources/parole-comuni.txt")
                .mapToPair(row -> new Tuple2<>(String.valueOf(row.charAt(0)), row.length()));
        JavaPairRDD<String, Iterable<Integer>> results = paroleComuni.groupByKey();

        results = results.cache();

        results.foreach(Logger::info);
        results.count();
        sc.close();
    }
}