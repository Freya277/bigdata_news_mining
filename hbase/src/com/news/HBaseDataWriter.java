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
    private static final String HDFS_AUTHOR_DIR = "hdfs://localhost:8020/spark_output/author_influence/";
    private static final String HDFS_CATEGORY_DIR = "hdfs://localhost:8020/spark_output/hot_categories/";
    private static final String HDFS_YEARLY_DIR = "hdfs://localhost:8020/hive_output/yearly_count/";
    private static final String HDFS_AVGLEN_DIR = "hdfs://localhost:8020/hive_output/headline_avglen/";

    public static void main(String[] args) {
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", "localhost");
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");

        Configuration hdfsConf = new Configuration();
        hdfsConf.set("fs.defaultFS", "hdfs://localhost:8020");

        try (Connection conn = ConnectionFactory.createConnection(hbaseConf);
             FileSystem fs = FileSystem.get(hdfsConf)) {

            writeAuthorInfluence(conn, fs, HDFS_AUTHOR_DIR);
            writeHotCategory(conn, fs, HDFS_CATEGORY_DIR);
            writeYearlyStats(conn, fs, HDFS_YEARLY_DIR);
            writeAvgHeadlineLen(conn, fs, HDFS_AVGLEN_DIR);

            System.out.println("Success！");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void writeAuthorInfluence(Connection conn, FileSystem fs, String dirPath) throws Exception {
        Table table = conn.getTable(TableName.valueOf("news_author_influence"));

        FileStatus[] statuses = fs.listStatus(new Path(dirPath));
        for (FileStatus status : statuses) {
            if (status.isFile()) {
                FSDataInputStream in = fs.open(status.getPath());
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\t");
                    if (parts.length != 2) continue;
                    String author = parts[0];
                    double score = Double.parseDouble(parts[1]);

                    Put put = new Put(Bytes.toBytes(author));
                    put.addColumn(Bytes.toBytes("profile"), Bytes.toBytes("influence_score"), Bytes.toBytes(score));
                    table.put(put);
                }
                reader.close();
            }
        }
        table.close();
    }

    private static void writeHotCategory(Connection conn, FileSystem fs, String dirPath) throws Exception {
        Table table = conn.getTable(TableName.valueOf("news_category_hot"));
        FileStatus[] statuses = fs.listStatus(new Path(dirPath));
        for (FileStatus status : statuses) {
            if (status.isFile() && status.getPath().getName().endsWith(".csv")) {
                FSDataInputStream in = fs.open(status.getPath());
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\t");
                    if (parts.length != 3) continue;
                    String category = parts[0];
                    int count = Integer.parseInt(parts[1]);
                    double avgLen = Double.parseDouble(parts[2]);
                    double hotScore = count * avgLen / 100;

                    Put put = new Put(Bytes.toBytes(category));
                    put.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("article_count"), Bytes.toBytes(count));
                    put.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("avg_headline_len"), Bytes.toBytes(avgLen));
                    put.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("hot_score"), Bytes.toBytes(hotScore));
                    table.put(put);
                }
                reader.close();
            }
        }
        table.close();
    }

    private static void writeYearlyStats(Connection conn, FileSystem fs, String dirPath) throws Exception {
        Table table = conn.getTable(TableName.valueOf("news_yearly_stats"));

        FileStatus[] statuses = fs.listStatus(new Path(dirPath));
        for (FileStatus status : statuses) {
            if (status.isFile()) {
                FSDataInputStream in = fs.open(status.getPath());
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\t");
                    if (parts.length != 2) continue;
                    String year = parts[0];
                    int count = Integer.parseInt(parts[1]);

                    Put put = new Put(Bytes.toBytes(year));
                    put.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("news_count"), Bytes.toBytes(count));
                    table.put(put);
                }
                reader.close();
            }
        }
        table.close();
    }

    private static void writeAvgHeadlineLen(Connection conn, FileSystem fs, String dirPath) throws Exception {
        Table table = conn.getTable(TableName.valueOf("news_yearly_stats"));
        FileStatus[] statuses = fs.listStatus(new Path(dirPath));
        for (FileStatus status : statuses) {
            if (status.isFile()) {
                FSDataInputStream in = fs.open(status.getPath());
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                double avgLen = Double.parseDouble(reader.readLine());
                reader.close();

                Put put = new Put(Bytes.toBytes("total"));
                put.addColumn(Bytes.toBytes("metrics"), Bytes.toBytes("avg_headline_len"), Bytes.toBytes(avgLen));
                table.put(put);
                break; 
            }
        }
        table.close();
    }
}