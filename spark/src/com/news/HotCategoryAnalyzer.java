package com.news;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class HotCategoryAnalyzer {
    public static Dataset<Row> analyzeHotCategories(SparkSession spark) {
        Dataset<Row> newsDF = spark.read()
                .option("delimiter", "\t")
                .schema("category STRING, date STRING, authors STRING, headline STRING, short_description STRING")
                .csv("/news_cleaned/part-r-00000");

        Dataset<Row> newsWithLenDF = newsDF.withColumn("headline_len", functions.length(functions.col("headline")));

        Dataset<Row> hotCategoryDF = newsWithLenDF.groupBy("category")
                .agg(
                    functions.count("category").alias("article_count"),
                    functions.avg("headline_len").alias("avg_headline_len")
                )
                .withColumn("hot_score", functions.col("article_count").multiply(functions.col("avg_headline_len")).divide(100))
                .orderBy(functions.col("hot_score").desc());

        return hotCategoryDF;
    }
}