package com.news;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class HBaseDataWriter {
    // HDFS paths for result directories
    private static final String HDFS_AUTHOR_DIR = "hdfs://localhost:8020/spark_output/author_influence/";
    private static final String HDFS_CATEGORY_DIR = "hdfs://localhost:8020/spark_output/hot_categories/";
    private static final String HDFS_YEARLY_DIR = "hdfs://localhost:8020/hive_output/yearly_count/";
    private static final String HDFS_AVGLEN_DIR = "hdfs://localhost:8020/hive_output/headline_avglen/";

    public static void main(String[] args) {
        // HBase configuration
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", "localhost");
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");

        // HDFS configuration
        Configuration hdfsConf = new Configuration();
        hdfsConf.set("fs.defaultFS", "hdfs://localhost:8020");

        try (Connection conn = ConnectionFactory.createConnection(hbaseConf);
             FileSystem fs = FileSystem.get(hdfsConf)) {

            System.out.println("===== Starting to read Spark Author Influence file =====");
            writeAuthorInfluence(conn, fs, HDFS_AUTHOR_DIR);
            System.out.println("===== Starting to read Spark Hot Category file =====");
            writeHotCategory(conn, fs, HDFS_CATEGORY_DIR);
            System.out.println("===== Starting to read Hive Yearly Stats file =====");
            writeYearlyStats(conn, fs, HDFS_YEARLY_DIR);
            System.out.println("===== Starting to read Hive Average Headline Length file =====");
            writeAvgHeadlineLen(conn, fs, HDFS_AVGLEN_DIR);

            System.out.println("All Spark/Hive results written to HBase successfully!");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void writeAuthorInfluence(Connection conn, FileSystem fs, String dirPath) throws Exception {
        Table table = conn.getTable(TableName.valueOf("news_author_influence"));
        FileStatus[] statuses = fs.listStatus(new Path(dirPath));
        System.out.println("Number of files in author influence directory: " + statuses.length);
        
        int lineCount = 0;
        for (FileStatus status : statuses) {
            if (status.isFile() && !status.getPath().getName().equals("_SUCCESS")) { // Skip _SUCCESS file
                System.out.println("Reading file: " + status.getPath().getName());
                FSDataInputStream in = fs.open(status.getPath());
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty() || !line.startsWith("(") || !line.endsWith(")")) {
                        continue; // Skip empty/non-standard lines
                    }

                    // Step 1: Remove leading/trailing parentheses
                    String content = line.substring(1, line.length() - 1);
                    // Step 2: Split by last comma (separating author and score)
                    int lastCommaIndex = content.lastIndexOf(",");
                    if (lastCommaIndex == -1) {
                        System.out.println("Invalid line (no last comma): " + line);
                        continue;
                    }
                    
                    // Step 3: Extract author name (trim) and score
                    String authorPart = content.substring(0, lastCommaIndex).trim();
                    String scoreStr = content.substring(lastCommaIndex + 1).trim();
                    
                    // Filter abnormal author names (avoid null/empty)
                    if (authorPart.isEmpty() || scoreStr.isEmpty()) {
                        continue;
                    }

                    try {
                        double score = Double.parseDouble(scoreStr);
                        // Author name might contain special characters, use directly as RowKey
                        Put put = new Put(Bytes.toBytes(authorPart));
                        put.addColumn(Bytes.toBytes("profile"), Bytes.toBytes("influence_score"), 
                                      Bytes.toBytes(score));
                        table.put(put);
                        lineCount++;
                    } catch (NumberFormatException e) {
                        System.out.println("Score format error: " + scoreStr + " | Line content: " + line);
                    }
                }
                reader.close();
            }
        }
        System.out.println("Successfully wrote author influence data lines: " + lineCount);
        table.close();
    }

    private static void writeHotCategory(Connection conn, FileSystem fs, String dirPath) throws Exception {
        Table table = conn.getTable(TableName.valueOf("news_category_hot"));
        FileStatus[] statuses = fs.listStatus(new Path(dirPath));
        System.out.println("Number of files in hot category directory: " + statuses.length);
        
        int lineCount = 0;
        for (FileStatus status : statuses) {
            if (status.isFile() && !status.getPath().getName().equals("_SUCCESS")) { // Skip _SUCCESS
                System.out.println("Reading file: " + status.getPath().getName());
                FSDataInputStream in = fs.open(status.getPath());
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;
                    
                    // Split by \t (4 columns: category, count, avg_len, hot_score)
                    String[] parts = line.split("\t");
                    if (parts.length != 4) {
                        System.out.println("Invalid line (wrong column count): " + line);
                        continue;
                    }
                    
                    try {
                        String category = parts[0].trim();
                        int articleCount = Integer.parseInt(parts[1].trim());
                        double avgLen = Double.parseDouble(parts[2].trim());
                        double hotScore = Double.parseDouble(parts[3].trim());
                        
                        Put put = new Put(Bytes.toBytes(category));
                        put.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("article_count"), 
                                      Bytes.toBytes(articleCount));
                        put.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("avg_headline_len"), 
                                      Bytes.toBytes(avgLen));
                        put.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("hot_score"), 
                                      Bytes.toBytes(hotScore));
                        table.put(put);
                        lineCount++;
                    } catch (NumberFormatException e) {
                        System.out.println("Number format error: " + line);
                    }
                }
                reader.close();
            }
        }
        System.out.println("Successfully wrote hot category data lines: " + lineCount);
        table.close();
    }

    private static void writeYearlyStats(Connection conn, FileSystem fs, String dirPath) throws Exception {
        Table table = conn.getTable(TableName.valueOf("news_yearly_stats"));
        FileStatus[] statuses = fs.listStatus(new Path(dirPath));
        System.out.println("Number of files in yearly stats directory: " + statuses.length);
        
        int lineCount = 0;
        for (FileStatus status : statuses) {
            if (status.isFile()) {
                System.out.println("Reading file: " + status.getPath().getName());
                FSDataInputStream in = fs.open(status.getPath());
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;
                    
                    String[] parts = line.contains("\t") ? line.split("\t") : line.split(",");
                    if (parts.length != 2) continue;
                    
                    try {
                        Put put = new Put(Bytes.toBytes(parts[0].trim()));
                        put.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("news_count"), 
                                      Bytes.toBytes(Integer.parseInt(parts[1].trim())));
                        table.put(put);
                        lineCount++;
                    } catch (NumberFormatException e) {
                        System.out.println("Number format error: " + line);
                    }
                }
                reader.close();
            }
        }
        System.out.println("Successfully wrote yearly stats data lines: " + lineCount);
        table.close();
    }

    private static void writeAvgHeadlineLen(Connection conn, FileSystem fs, String dirPath) throws Exception {
        Table table = conn.getTable(TableName.valueOf("news_yearly_stats"));
        FileStatus[] statuses = fs.listStatus(new Path(dirPath));
        System.out.println("Number of files in average headline length directory: " + statuses.length);
        
        for (FileStatus status : statuses) {
            if (status.isFile()) {
                System.out.println("Reading file: " + status.getPath().getName());
                FSDataInputStream in = fs.open(status.getPath());
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                String line = reader.readLine().trim();
                reader.close();
                
                try {
                    double avgLen = Double.parseDouble(line);
                    Put put = new Put(Bytes.toBytes("total"));
                    put.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("avg_headline_len"), 
                                  Bytes.toBytes(avgLen));
                    table.put(put);
                    System.out.println("Successfully wrote average headline length: " + avgLen);
                } catch (NumberFormatException e) {
                    System.out.println("Number format error: " + line);
                }
                break;
            }
        }
        table.close();
    }
}