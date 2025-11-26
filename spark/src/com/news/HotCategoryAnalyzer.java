package com.news;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class HotCategoryAnalyzer {
    public static Dataset<Row> analyzeHotCategories(SparkSession spark) {
        // Manually build schema (Spark 2.1.3 does not support string schema)
        StructType newsSchema = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("category", DataTypes.StringType, true),
            DataTypes.createStructField("date", DataTypes.StringType, true),
            DataTypes.createStructField("authors", DataTypes.StringType, true),
            DataTypes.createStructField("headline", DataTypes.StringType, true),
            DataTypes.createStructField("short_description", DataTypes.StringType, true)
        });

        // Read data with custom schema
        Dataset<Row> newsDF = spark.read()
                .option("delimiter", "\t")
                .schema(newsSchema)
                .csv("/news_cleaned/part-r-00000");

        // Add headline length column
        Dataset<Row> newsWithLenDF = newsDF.withColumn("headline_len", functions.length(functions.col("headline")));

        // Calculate hot score: article_count * avg_headline_len / 100
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