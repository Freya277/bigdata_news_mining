package com.news;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class SparkMain {
    public static void main(String[] args) {
        // Spark configuration (compatible with Spark 2.1.3)
        SparkConf conf = new SparkConf()
                .setAppName("NewsBigDataAnalysis")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        try {
            // 1. Author Influence Analysis (return JavaPairRDD)
            JavaPairRDD<String, Double> authorInfluenceRDD = AuthorInfluenceAnalyzer.calculateInfluence(sc);
            authorInfluenceRDD.map(new Tuple2<String, Double>() {
                @Override
                public String toString() {
                    return _1 + "\t" + _2;
                }
            }).saveAsTextFile("/spark_output/author_influence");

            // 2. Hot Category Analysis
            Dataset<Row> hotCategoryDF = HotCategoryAnalyzer.analyzeHotCategories(spark);
            hotCategoryDF.write()
                    .mode("overwrite")
                    .option("delimiter", "\t")
                    .csv("/spark_output/hot_categories");

            // 3. Headline TF-IDF Extraction
            Dataset<Row> tfidfDF = HeadlineTFIDF.extractTFIDF(spark);
            tfidfDF.write()
                    .mode("overwrite")
                    .parquet("/spark_output/headline_tfidf");

            System.out.println("Spark analysis completed successfully!");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sc.stop();
            spark.stop();
        }
    }
}