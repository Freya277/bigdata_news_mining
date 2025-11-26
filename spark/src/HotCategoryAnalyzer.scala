package spark

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object HotCategoryAnalyzer {
  def analyzeHotCategories(spark: SparkSession): DataFrame = {
    // Read cleaned data from HDFS and create DataFrame
    val newsDF = spark.read
      .option("delimiter", "\t")
      .schema("category STRING, date STRING, authors STRING, headline STRING, short_description STRING")
      .csv("/news_cleaned/part-r-00000")
    
    // Add headline length column
    val newsWithLenDF = newsDF.withColumn("headline_len", length(col("headline")))
    
    // Calculate category metrics: article count + average headline length
    val categoryMetricsDF = newsWithLenDF.groupBy("category")
      .agg(
        count("category").alias("article_count"),
        avg("headline_len").alias("avg_headline_len")
      )
    
    // Calculate hot score: article_count * avg_headline_len / 100
    val hotCategoryDF = categoryMetricsDF.withColumn(
      "hot_score",
      col("article_count") * col("avg_headline_len") / 100
    ).orderBy(col("hot_score").desc)
    
    hotCategoryDF
  }
}