package org.sparkcourse;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.tinylog.Logger;

import java.util.List;

public class App {

    public static void main( String[] args ) {
        Logger.info("Start spark app");

        List<Double> list = List.of(11d, 112d, 113d, 114d);

        SparkConf sparkConf = new SparkConf()
                .setAppName("Spark Course App")
                .setMaster("local[*]"); // use all the available cores on the machine
        try (var sc = new JavaSparkContext(sparkConf)) {
            JavaRDD<Double> rdd = sc.parallelize(list);

            double reduced = rdd.reduce((a, b) -> a + b);
            Logger.info("reduced={}", reduced);

            JavaRDD<Double> sqrtRdd = rdd.map(d -> d >= 0 ? Math.sqrt(d) : null);
            sqrtRdd.foreach(Logger::info);

            // count the elements
            Logger.info("count={}", sqrtRdd.count()); // it's not good in the middle of a task, it's good at the end of a process
            long count = sqrtRdd.map(v->1L).reduce((a, b) -> a + b);
            Logger.info("reduceCount={}", count);
        } catch (RuntimeException e) {
            Logger.error(e);
        }
    }
}
