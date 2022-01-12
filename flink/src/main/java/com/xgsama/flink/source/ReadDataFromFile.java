package com.xgsama.flink.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * ReadDataFromFile
 *
 * @author : xgSama
 * @date : 2022/1/12 10:28:07
 */
public class ReadDataFromFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String filePath = "/Users/xgSama/IdeaProjects/bigdata/input/sensor.txt";

        DataStreamSource<String> fileSource1 = env.readTextFile(filePath, "utf8");
        fileSource1.print();


        env.execute();
    }
}
