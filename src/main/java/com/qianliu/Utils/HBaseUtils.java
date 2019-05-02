package com.qianliu.Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HBaseUtils {

    public static Configuration configuration;
    public static Connection con;
    static {
        configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", "192.168.48.138:2181");
        configuration.set("hbase.rootdir", "hdfs://192.168.48.138:8020/hbase");
        try {
            con = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
