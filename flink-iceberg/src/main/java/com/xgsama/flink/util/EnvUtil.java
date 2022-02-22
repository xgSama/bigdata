package com.xgsama.flink.util;

import org.apache.hadoop.conf.Configuration;

/**
 * EnvUtil
 *
 * @author : xgSama
 * @date : 2022/2/16 17:26:43
 */
public class EnvUtil {

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

    public static String createHiveCatalogSql() {
        return
                "CREATE CATALOG hive_catalog WITH (\n" +
                        "  'type'='iceberg',\n" +
                        "  'catalog-type'='hive',\n" +
                        "  'uri'='thrift://172.16.101.237:9083',\n" +
                        "  'clients'='5',\n" +
                        "  'property-version'='1',\n" +
                        "  'warehouse'='hdfs://ns1/user/xgsama/warehouse/'\n" +
                        ")";
    }
}
