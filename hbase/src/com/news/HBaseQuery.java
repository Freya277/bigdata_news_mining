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

import java.io.FileWriter;
import java.io.PrintWriter;

public class HBaseQuery {
    // Define the report output path
    private static final String REPORT_PATH = "~/news-bigdata-project/hbase/hbase_result/query_report.txt";

    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        // Initialize file writer
        try (PrintWriter writer = new PrintWriter(new FileWriter(REPORT_PATH));
             Connection conn = ConnectionFactory.createConnection(conf)) {

            writer.println("===== HBase Query Results (Readable Report) =====\n");

            // 1. Query author influence (Lee Moran)
            writer.println("1. Author Influence Query (Lee Moran)");
            queryAuthorInfluence(conn, "Lee Moran", writer);

            // 2. Query hot category (POLITICS)
            writer.println("\n2. Hot News Category Query (POLITICS)");
            queryHotCategory(conn, "POLITICS", writer);

            // 3. Query news count for year 2022
            writer.println("\n3. Yearly News Count Query (2022)");
            queryYearlyStats(conn, "2022", writer);

            // 4. Query average headline length
            writer.println("\n4. Average Headline Length Query");
            queryAvgHeadlineLen(conn, writer);

            writer.println("\n===== Query Complete =====");
            System.out.println("Readable report generated: " + REPORT_PATH);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Author influence query (Write to file + Console output)
    private static void queryAuthorInfluence(Connection conn, String author, PrintWriter writer) throws Exception {
        Table table = conn.getTable(TableName.valueOf("news_author_influence"));
        Get get = new Get(Bytes.toBytes(author));
        Result result = table.get(get);
        if (!result.isEmpty()) {
            double score = Bytes.toDouble(result.getValue(Bytes.toBytes("profile"), Bytes.toBytes("influence_score")));
            String res = "Author: " + author + " | Influence Score: " + score;
            writer.println(res);
            System.out.println(res);
        } else {
            String res = "No data found for author \"" + author + "\"";
            writer.println(res);
            System.out.println(res);
        }
        table.close();
    }

    // Hot category query (Write to file + Console output)
    private static void queryHotCategory(Connection conn, String category, PrintWriter writer) throws Exception {
        Table table = conn.getTable(TableName.valueOf("news_category_hot"));
        Get get = new Get(Bytes.toBytes(category));
        Result result = table.get(get);
        if (!result.isEmpty()) {
            int articleCount = Bytes.toInt(result.getValue(Bytes.toBytes("stats"), Bytes.toBytes("article_count")));
            double hotScore = Bytes.toDouble(result.getValue(Bytes.toBytes("stats"), Bytes.toBytes("hot_score")));
            double avgLen = Bytes.toDouble(result.getValue(Bytes.toBytes("stats"), Bytes.toBytes("avg_headline_len")));
            writer.println("Category: " + category);
            writer.println("  - Article Count: " + articleCount);
            writer.println("  - Avg Headline Length: " + avgLen);
            writer.println("  - Hot Score: " + hotScore);
            // Console sync output
            System.out.println("Category: " + category + " | Article Count: " + articleCount + " | Hot Score: " + hotScore);
        } else {
            String res = "No data found for category \"" + category + "\"";
            writer.println(res);
            System.out.println(res);
        }
        table.close();
    }

    // Yearly stats query (Write to file + Console output)
    private static void queryYearlyStats(Connection conn, String year, PrintWriter writer) throws Exception {
        Table table = conn.getTable(TableName.valueOf("news_yearly_stats"));
        Get get = new Get(Bytes.toBytes(year));
        Result result = table.get(get);
        if (!result.isEmpty()) {
            int count = Bytes.toInt(result.getValue(Bytes.toBytes("metrics"), Bytes.toBytes("news_count")));
            String res = "Year: " + year + " | News Count: " + count;
            writer.println(res);
            System.out.println(res);
        } else {
            String res = "No news count data found for year \"" + year + "\"";
            writer.println(res);
            System.out.println(res);
        }
        table.close();
    }

    // Average headline length query (Write to file + Console output)
    private static void queryAvgHeadlineLen(Connection conn, PrintWriter writer) throws Exception {
        Table table = conn.getTable(TableName.valueOf("news_yearly_stats"));
        Get get = new Get(Bytes.toBytes("total"));
        Result result = table.get(get);
        if (!result.isEmpty()) {
            double avgLen = Bytes.toDouble(result.getValue(Bytes.toBytes("metrics"), Bytes.toBytes("avg_headline_len")));
            String res = "Overall Average Headline Length: " + avgLen + " characters";
            writer.println(res);
            System.out.println(res);
        } else {
            String res = "No average headline length data found";
            writer.println(res);
            System.out.println(res);
        }
        table.close();
    }
}