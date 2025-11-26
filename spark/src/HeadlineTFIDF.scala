package spark

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.ml.feature.{Tokenizer, HashingTF, IDF}

object HeadlineTFIDF {
  def extractTFIDF(spark: SparkSession): DataFrame = {
    // Load headline data
    val newsDF = spark.read
      .option("delimiter", "\t")
      .schema("category STRING, date STRING, authors STRING, headline STRING, short_description STRING")
      .csv("/news_cleaned/part-r-00000")
    
    // Filter empty headlines
    val validHeadlineDF = newsDF.filter(col("headline").isNotNull && col("headline") =!= "")
    
    // Step 1: Tokenize headline (split into words)
    val tokenizer = new Tokenizer()
      .setInputCol("headline")
      .setOutputCol("words")
    val wordsDF = tokenizer.transform(validHeadlineDF)
    
    // Step 2: Calculate TF (Term Frequency)
    val hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("raw_features")
      .setNumFeatures(2000)  // Feature dimension
    val tfDF = hashingTF.transform(wordsDF)
    
    // Step 3: Calculate IDF (Inverse Document Frequency)
    val idf = new IDF()
      .setInputCol("raw_features")
      .setOutputCol("tfidf_features")
    val idfModel = idf.fit(tfDF)
    val tfidfDF = idfModel.transform(tfDF)
    
    // Select result columns
    val resultDF = tfidfDF.select("headline", "tfidf_features")
    resultDF
  }
}