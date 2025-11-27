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

public class HiveResultToHBase {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("HiveResultToHBase")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", "localhost");
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");

        // 1. Read yearly count from HDFS (Hive output)
        String yearlyPath = "hdfs://localhost:8020/hive_output/yearly_count";
        JavaRDD<String> yearlyRDD = sc.textFile(yearlyPath);
        yearlyRDD.foreachPartition(partition -> {
            try (Connection conn = ConnectionFactory.createConnection(hbaseConf);
                 Table table = conn.getTable(TableName.valueOf("news_headline_stats"))) {
                while (partition.hasNext()) {
                    String line = partition.next();
                    String[] parts = line.split("\t");
                    if (parts.length != 2) continue;
                    String year = parts[0];
                    int newsCount = Integer.parseInt(parts[1]);

                    Put put = new Put(Bytes.toBytes(year));
                    put.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("news_count"), Bytes.toBytes(newsCount));
                    table.put(put);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // 2. Read headline avg len from HDFS (Hive output)
        String avgLenPath = "hdfs://localhost:8020/hive_output/headline_avglen";
        JavaRDD<String> avgLenRDD = sc.textFile(avgLenPath);
        double avgLen = Double.parseDouble(avgLenRDD.collect().get(0));

        // Write avg headline len to HBase (RowKey: "total")
        try (Connection conn = ConnectionFactory.createConnection(hbaseConf);
             Table table = conn.getTable(TableName.valueOf("news_headline_stats"))) {
            Put put = new Put(Bytes.toBytes("total"));
            put.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("avg_headline_len"), Bytes.toBytes(avgLen));
            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Hive results written to HBase successfully!");
        sc.stop();
    }
}