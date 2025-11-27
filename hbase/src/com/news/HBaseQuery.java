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
        
            queryAuthorInfluence(conn, "Lee Moran");
       
            queryHotCategory(conn, "POLITICS");
         
            queryYearlyStats(conn, "2022");
       
            queryAvgHeadlineLen(conn);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void queryAuthorInfluence(Connection conn, String author) throws Exception {
        Table table = conn.getTable(TableName.valueOf("news_author_influence"));
        Get get = new Get(Bytes.toBytes(author));
        Result result = table.get(get);
        if (!result.isEmpty()) {
            double score = Bytes.toDouble(result.getValue(Bytes.toBytes("profile"), Bytes.toBytes("influence_score")));
            System.out.println("Author " + author + "'s Influence Score: " + score);
        } else {
            System.out.println("No data found for author " + author);
        }
        table.close();
    }

    private static void queryHotCategory(Connection conn, String category) throws Exception {
        Table table = conn.getTable(TableName.valueOf("news_category_hot"));
        Get get = new Get(Bytes.toBytes(category));
        Result result = table.get(get);
        if (!result.isEmpty()) {
            int count = Bytes.toInt(result.getValue(Bytes.toBytes("stats"), Bytes.toBytes("article_count")));
            double hotScore = Bytes.toDouble(result.getValue(Bytes.toBytes("stats"), Bytes.toBytes("hot_score")));
            System.out.println("Category " + category + " - Article Count: " + count + ", Hot Score: " + hotScore);
        }
        table.close();
    }

    private static void queryYearlyStats(Connection conn, String year) throws Exception {
        Table table = conn.getTable(TableName.valueOf("news_yearly_stats"));
        Get get = new Get(Bytes.toBytes(year));
        Result result = table.get(get);
        if (!result.isEmpty()) {
            int count = Bytes.toInt(result.getValue(Bytes.toBytes("metrics"), Bytes.toBytes("news_count")));
            System.out.println("News count for year " + year + ": " + count);
        }
        table.close();
    }

    private static void queryAvgHeadlineLen(Connection conn) throws Exception {
        Table table = conn.getTable(TableName.valueOf("news_yearly_stats"));
        Get get = new Get(Bytes.toBytes("total"));
        Result result = table.get(get);
        if (!result.isEmpty()) {
            double avgLen = Bytes.toDouble(result.getValue(Bytes.toBytes("metrics"), Bytes.toBytes("avg_headline_len")));
            System.out.println("Overall Average Headline Length: " + avgLen);
        }
        table.close();
    }
}