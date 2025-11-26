package com.news;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class SparkMain {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("NewsBigDataAnalysis")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // 1. Author Influence Analysis
        JavaRDD<Tuple2<String, Double>> authorInfluenceRDD = AuthorInfluenceAnalyzer.calculateInfluence(sc);
        authorInfluenceRDD.map(t -> t._1 + "\t" + t._2).saveAsTextFile("/spark_output/author_influence");

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

        sc.stop();
        spark.stop();
    }
}