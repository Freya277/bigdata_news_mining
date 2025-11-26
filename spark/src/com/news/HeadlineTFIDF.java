package com.news;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;

public class HeadlineTFIDF {
    public static Dataset<Row> extractTFIDF(SparkSession spark) {
        // Manually build schema
        StructType newsSchema = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("category", DataTypes.StringType, true),
            DataTypes.createStructField("date", DataTypes.StringType, true),
            DataTypes.createStructField("authors", DataTypes.StringType, true),
            DataTypes.createStructField("headline", DataTypes.StringType, true),
            DataTypes.createStructField("short_description", DataTypes.StringType, true)
        });

        // Read data and filter empty headlines
        Dataset<Row> newsDF = spark.read()
                .option("delimiter", "\t")
                .schema(newsSchema)
                .csv("hdfs://localhost:8020/news_cleaned/part-r-00000");

        Dataset<Row> validHeadlineDF = newsDF.filter(
            functions.col("headline").isNotNull().and(functions.col("headline").notEqual(""))
        );

        // Tokenize headline
        Tokenizer tokenizer = new Tokenizer()
                .setInputCol("headline")
                .setOutputCol("words");
        Dataset<Row> wordsDF = tokenizer.transform(validHeadlineDF);

        // Calculate TF
        HashingTF hashingTF = new HashingTF()
                .setInputCol("words")
                .setOutputCol("raw_features")
                .setNumFeatures(2000);
        Dataset<Row> tfDF = hashingTF.transform(wordsDF);

        // Calculate IDF
        IDF idf = new IDF().setInputCol("raw_features").setOutputCol("tfidf_features");
        IDFModel idfModel = idf.fit(tfDF);
        Dataset<Row> tfidfDF = idfModel.transform(tfDF);

        // Return headline + TF-IDF features
        return tfidfDF.select("headline", "tfidf_features");
    }
}