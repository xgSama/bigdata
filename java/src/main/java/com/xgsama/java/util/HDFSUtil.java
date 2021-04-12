package com.xgsama.java.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

/**
 * HDFSUtil
 *
 * @author xgSama
 * @date 2020/11/13 17:05
 */
public class HDFSUtil {

    private FileSystem fs = null;

    public HDFSUtil(String uri, String user) throws IOException, InterruptedException {
        fs = FileSystem.get(URI.create(uri), null, user);
    }

    public HDFSUtil(String uri) throws IOException, InterruptedException {
        this(uri, "hadoop");
    }

    public static void main(String[] args) throws Exception {

        String hdfsPath = "hdfs://app2:9000";
        String file = "/tmp/abc.csv";
        String user = "hadoop";

        String sourcePath = args[0];
        String kafkaServer = args[1];
        String topic = args[2];


        KafkaProducerUtil kp = new KafkaProducerUtil(kafkaServer);

        String str;

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(new URI(hdfsPath), conf, user);

        FSDataInputStream in = fs.open(new Path(file));

        BufferedReader d = new BufferedReader(new InputStreamReader(in));
        String line = null;
        while ((line = d.readLine()) != null) {
            System.out.println(line);

        }

        d.close();
        in.close();
        fs.close();



    }


    public static void cat(String hdfsPath, String file, String user) throws Exception {

        FileSystem fs = FileSystem.get(new URI(hdfsPath), null, user);

        FSDataInputStream in = fs.open(new Path(file));

        BufferedReader d = new BufferedReader(new InputStreamReader(in));
        String line = null;
        while ((line = d.readLine()) != null) {
            System.out.println(line);

        }

        d.close();
        in.close();
        fs.close();
    }

}
