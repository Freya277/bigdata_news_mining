package com.news;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SparkResultToHBase {
    public static void main(String[] args) {
        // Spark configuration
        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkResultToHBase")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // HBase configuration
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", "localhost");
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");

        // 1. Read author influence from HDFS (Spark output)
        String authorPath = "hdfs://localhost:9000/spark_output/author_influence";
        JavaRDD<String> authorRDD = sc.textFile(authorPath);
        authorRDD.foreachPartition(partition -> {
            try (Connection conn = ConnectionFactory.createConnection(hbaseConf);
                 Table table = conn.getTable(TableName.valueOf("news_author_influence"))) {
                while (partition.hasNext()) {
                    String line = partition.next();
                    String[] parts = line.split("\t");
                    if (parts.length != 2) continue;
                    String author = parts[0];
                    double influenceScore = Double.parseDouble(parts[1]);

                    // Put data to HBase
                    Put put = new Put(Bytes.toBytes(author));
                    put.addColumn(Bytes.toBytes("profile"), Bytes.toBytes("influence_score"), Bytes.toBytes(influenceScore));
                    table.put(put);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // 2. Read hot category from HDFS (Spark output)
        String categoryPath = "hdfs://localhost:9000/spark_output/hot_categories";
        JavaRDD<String> categoryRDD = sc.textFile(categoryPath);
        categoryRDD.foreachPartition(partition -> {
            try (Connection conn = ConnectionFactory.createConnection(hbaseConf);
                 Table table = conn.getTable(TableName.valueOf("news_category_hot"))) {
                while (partition.hasNext()) {
                    String line = partition.next();
                    String[] parts = line.split("\t");
                    if (parts.length != 3) continue;
                    String category = parts[0];
                    int articleCount = Integer.parseInt(parts[1]);
                    double avgLen = Double.parseDouble(parts[2]);
                    double hotScore = articleCount * avgLen / 100;

                    // Put data to HBase
                    Put put = new Put(Bytes.toBytes(category));
                    put.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("article_count"), Bytes.toBytes(articleCount));
                    put.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("avg_headline_len"), Bytes.toBytes(avgLen));
                    put.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("hot_score"), Bytes.toBytes(hotScore));
                    table.put(put);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        System.out.println("Spark results written to HBase successfully!");
        sc.stop();
    }
}