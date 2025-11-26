package spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkMain {
  def main(args: Array[String]): Unit = {
    // Spark configuration
    val conf = new SparkConf()
      .setAppName("NewsBigDataAnalysis")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    
    // 1. Author influence analysis (Core RDD)
    val authorInfluenceRDD = AuthorInfluenceAnalyzer.calculateInfluence(sc)
    authorInfluenceRDD.saveAsTextFile("/spark_output/author_influence")
    
    // 2. Hot category analysis (DataFrame/SQL)
    val hotCategoryDF = HotCategoryAnalyzer.analyzeHotCategories(spark)
    hotCategoryDF.write
      .mode("overwrite")
      .option("delimiter", "\t")
      .csv("/spark_output/hot_categories")
    
    // 3. Headline TF-IDF extraction (MLlib)
    val tfidfDF = HeadlineTFIDF.extractTFIDF(spark)
    tfidfDF.write
      .mode("overwrite")
      .parquet("/spark_output/headline_tfidf")
    
    // Stop Spark context
    sc.stop()
    spark.stop()
  }
}