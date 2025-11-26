package com.news;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class AuthorInfluenceAnalyzer {
    public static JavaPairRDD<String, Double> calculateInfluence(JavaSparkContext sc) {
        // Load data and filter invalid lines
        JavaRDD<String> newsRDD = sc.textFile("/news_cleaned/part-r-00000");
        JavaRDD<String> validNewsRDD = newsRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String line) throws Exception {
                String[] fields = line.split("\t");
                return fields.length == 5 && !fields[2].isEmpty() && !fields[3].isEmpty();
            }
        });

        // Map to (author, (1, headlineLen)) -> JavaPairRDD (key: author, value: Tuple2)
        JavaPairRDD<String, Tuple2<Integer, Integer>> authorStatsRDD = validNewsRDD.mapToPair(
            new PairFunction<String, String, Tuple2<Integer, Integer>>() {
                @Override
                public Tuple2<String, Tuple2<Integer, Integer>> call(String line) throws Exception {
                    String[] fields = line.split("\t");
                    String author = fields[2];
                    int headlineLen = fields[3].length();
                    return new Tuple2<>(author, new Tuple2<>(1, headlineLen));
                }
            }
        );

        // Reduce by key: aggregate (totalCount, totalLen)
        JavaPairRDD<String, Tuple2<Integer, Integer>> authorAggRDD = authorStatsRDD.reduceByKey(
            new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                @Override
                public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
                    int totalCount = v1._1 + v2._1;
                    int totalLen = v1._2 + v2._2;
                    return new Tuple2<>(totalCount, totalLen);
                }
            }
        );

        // Calculate influence score: (totalCount * avgLen) / 100
        JavaPairRDD<String, Double> authorInfluenceRDD = authorAggRDD.mapValues(
            new Function<Tuple2<Integer, Integer>, Double>() {
                @Override
                public Double call(Tuple2<Integer, Integer> value) throws Exception {
                    int count = value._1;
                    int totalLen = value._2;
                    double avgLen = (double) totalLen / count;
                    return count * (avgLen / 100);
                }
            }
        );

        // Sort by influence score (descending)
        JavaPairRDD<String, Double> sortedInfluenceRDD = authorInfluenceRDD.sortByKey(false);

        return sortedInfluenceRDD;
    }
}