package com.news;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseQuery {
    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        try (Connection conn = ConnectionFactory.createConnection(conf)) {
            // 1. Query author influence (example: Lee Moran)
            Table authorTable = conn.getTable(TableName.valueOf("news_author_influence"));
            Get authorGet = new Get(Bytes.toBytes("Lee Moran"));
            Result authorResult = authorTable.get(authorGet);
            if (!authorResult.isEmpty()) {
                double influence = Bytes.toDouble(authorResult.getValue(Bytes.toBytes("profile"), Bytes.toBytes("influence_score")));
                System.out.println("Lee Moran's influence score: " + influence);
            }

            // 2. Query hot category (example: POLITICS)
            Table categoryTable = conn.getTable(TableName.valueOf("news_category_hot"));
            Get categoryGet = new Get(Bytes.toBytes("POLITICS"));
            Result categoryResult = categoryTable.get(categoryGet);
            if (!categoryResult.isEmpty()) {
                int articleCount = Bytes.toInt(categoryResult.getValue(Bytes.toBytes("stats"), Bytes.toBytes("article_count")));
                double hotScore = Bytes.toDouble(categoryResult.getValue(Bytes.toBytes("stats"), Bytes.toBytes("hot_score")));
                System.out.println("POLITICS article count: " + articleCount + ", hot score: " + hotScore);
            }

            // 3. Query headline avg len (RowKey: total)
            Table headlineTable = conn.getTable(TableName.valueOf("news_headline_stats"));
            Get headlineGet = new Get(Bytes.toBytes("total"));
            Result headlineResult = headlineTable.get(headlineGet);
            if (!headlineResult.isEmpty()) {
                double avgLen = Bytes.toDouble(headlineResult.getValue(Bytes.toBytes("metrics"), Bytes.toBytes("avg_headline_len")));
                System.out.println("Total avg headline length: " + avgLen);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}