package com.news;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTableCreator {
    public static void main(String[] args) {
        // HBase configuration
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        try (Connection conn = ConnectionFactory.createConnection(conf);
             Admin admin = conn.getAdmin()) {

            // 1. Create table: news_author_influence
            TableName authorTable = TableName.valueOf("news_author_influence");
            ColumnFamilyDescriptor authorCF = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("profile")).build();
            TableDescriptor authorTableDesc = TableDescriptorBuilder.newBuilder(authorTable)
                    .setColumnFamily(authorCF)
                    .build();
            if (!admin.tableExists(authorTable)) {
                admin.createTable(authorTableDesc);
                System.out.println("Table news_author_influence created successfully!");
            } else {
                System.out.println("Table news_author_influence already exists!");
            }

            // 2. Create table: news_category_hot
            TableName categoryTable = TableName.valueOf("news_category_hot");
            ColumnFamilyDescriptor categoryCF = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("stats")).build();
            TableDescriptor categoryTableDesc = TableDescriptorBuilder.newBuilder(categoryTable)
                    .setColumnFamily(categoryCF)
                    .build();
            if (!admin.tableExists(categoryTable)) {
                admin.createTable(categoryTableDesc);
                System.out.println("Table news_category_hot created successfully!");
            } else {
                System.out.println("Table news_category_hot already exists!");
            }

            // 3. Create table: news_headline_stats
            TableName headlineTable = TableName.valueOf("news_headline_stats");
            ColumnFamilyDescriptor headlineCF = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("metrics")).build();
            TableDescriptor headlineTableDesc = TableDescriptorBuilder.newBuilder(headlineTable)
                    .setColumnFamily(headlineCF)
                    .build();
            if (!admin.tableExists(headlineTable)) {
                admin.createTable(headlineTableDesc);
                System.out.println("Table news_headline_stats created successfully!");
            } else {
                System.out.println("Table news_headline_stats already exists!");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}