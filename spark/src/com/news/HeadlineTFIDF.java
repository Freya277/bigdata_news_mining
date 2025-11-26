package com.news;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;

public class HeadlineTFIDF {
    public static Dataset<Row> extractTFIDF(SparkSession spark) {
        Dataset<Row> newsDF = spark.read()
                .option("delimiter", "\t")
                .schema("category STRING, date STRING, authors STRING, headline STRING, short_description STRING")
                .csv("/news_cleaned/part-r-00000");

        Dataset<Row> validHeadlineDF = newsDF.filter(functions.col("headline").isNotNull().and(functions.col("headline").notEqual("")));

        Tokenizer tokenizer = new Tokenizer()
                .setInputCol("headline")
                .setOutputCol("words");
        Dataset<Row> wordsDF = tokenizer.transform(validHeadlineDF);

        HashingTF hashingTF = new HashingTF()
                .setInputCol("words")
                .setOutputCol("raw_features")
                .setNumFeatures(2000);
        Dataset<Row> tfDF = hashingTF.transform(wordsDF);

        IDF idf = new IDF().setInputCol("raw_features").setOutputCol("tfidf_features");
        IDFModel idfModel = idf.fit(tfDF);
        Dataset<Row> tfidfDF = idfModel.transform(tfDF);

        return tfidfDF.select("headline", "tfidf_features");
    }
}