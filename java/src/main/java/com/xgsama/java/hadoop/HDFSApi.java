package com.xgsama.java.hadoop;

import com.xgsama.java.util.HDFSUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * HDFSApi
 *
 * @author : xgSama
 * @date : 2022/2/15 09:43:19
 */
public class HDFSApi {

    public static void main(String[] args) throws Exception {
        System.getProperties().put("HADOOP_USER_NAME", "admin");

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://172.16.104.67:9000");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        RemoteIterator<LocatedFileStatus> fs = FileSystem.get(conf).listFiles(new Path("/user/admin/"), false);

        while (fs.hasNext()) {
            System.out.println(fs.next().getPath().getName());
        }
    }
}
