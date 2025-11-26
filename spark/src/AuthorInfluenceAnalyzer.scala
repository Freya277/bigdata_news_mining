package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

object AuthorInfluenceAnalyzer {
  def calculateInfluence(sc: SparkContext): RDD[(String, Double)] = {
    // Load cleaned news data from HDFS
    val newsRDD: RDD[String] = sc.textFile("/news_cleaned/part-r-00000")
    
    // Filter invalid lines (empty or incomplete fields)
    val validNewsRDD: RDD[String] = newsRDD.filter(line => {
      val fields = line.split("\t")
      fields.length == 5 && !fields(2).isEmpty && !fields(3).isEmpty
    })
    
    // Map: (author, (article_count=1, headline_length))
    val authorStatsRDD: RDD[(String, (Int, Int))] = validNewsRDD.map(line => {
      val fields = line.split("\t")
      val author = fields(2)
      val headlineLen = fields(3).length
      (author, (1, headlineLen))
    })
    
    // Reduce: (author, (total_articles, total_headline_length))
    val authorAggRDD: RDD[(String, (Int, Int))] = authorStatsRDD.reduceByKey((v1, v2) => {
      (v1._1 + v2._1, v1._2 + v2._2)
    })
    
    // Calculate influence score: total_articles * (avg_headline_length / 100)
    val authorInfluenceRDD: RDD[(String, Double)] = authorAggRDD.map { case (author, (count, totalLen)) =>
      val avgLen = totalLen.toDouble / count
      val influence = count * (avgLen / 100)
      (author, influence)
    }.sortBy(_._2, ascending = false)  // Sort by influence score descending
    
    authorInfluenceRDD
  }
}