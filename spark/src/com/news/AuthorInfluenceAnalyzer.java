package com.news;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class AuthorInfluenceAnalyzer {
    public static JavaRDD<Tuple2<String, Double>> calculateInfluence(JavaSparkContext sc) {
        JavaRDD<String> newsRDD = sc.textFile("/news_cleaned/part-r-00000");

        JavaRDD<String> validNewsRDD = newsRDD.filter(line -> {
            String[] fields = line.split("\t");
            return fields.length == 5 && !fields[2].isEmpty() && !fields[3].isEmpty();
        });

        JavaRDD<Tuple2<String, Tuple2<Integer, Integer>>> authorStatsRDD = validNewsRDD.mapToPair(
            (PairFunction<String, String, Tuple2<Integer, Integer>>) line -> {
                String[] fields = line.split("\t");
                String author = fields[2];
                int headlineLen = fields[3].length();
                return new Tuple2<>(author, new Tuple2<>(1, headlineLen));
            }
        );

        JavaRDD<Tuple2<String, Tuple2<Integer, Integer>>> authorAggRDD = authorStatsRDD.reduceByKey(
            (Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>) (v1, v2) -> {
                int totalCount = v1._1 + v2._1;
                int totalLen = v1._2 + v2._2;
                return new Tuple2<>(totalCount, totalLen);
            }
        );

        JavaRDD<Tuple2<String, Double>> authorInfluenceRDD = authorAggRDD.map(tuple -> {
            String author = tuple._1;
            int count = tuple._2._1;
            int totalLen = tuple._2._2;
            double avgLen = (double) totalLen / count;
            double influence = count * (avgLen / 100);
            return new Tuple2<>(author, influence);
        }).sortBy(Tuple2::_2, false);

        return authorInfluenceRDD;
    }
}