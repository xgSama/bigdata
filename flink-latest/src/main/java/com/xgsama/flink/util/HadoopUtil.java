package com.xgsama.flink.util;

import org.apache.hadoop.conf.Configuration;

/**
 * HadoopUtil
 *
 * @author : xgSama
 * @date : 2022/2/15 11:17:25
 */
public class HadoopUtil {

    public static void setHadoopUserName(String userName) {
        System.getProperties().put("HADOOP_USER_NAME", userName);
    }

    public static Configuration getDtTestConfiguration() {
        Configuration conf = new Configuration();
        conf.set("dfs.ha.namenodes.ns1", "nn1,nn2");
        conf.set("dfs.client.failover.proxy.provider.ns1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        conf.set("dfs.namenode.rpc-address.ns1.nn2", "172.16.104.69:9000");
        conf.set("dfs.namenode.rpc-address.ns1.nn1", "172.16.104.67:9000");
        conf.set("dfs.nameservices", "ns1");
        conf.set("fs.defaultFS", "hdfs://ns1");

        return conf;
    }
}
