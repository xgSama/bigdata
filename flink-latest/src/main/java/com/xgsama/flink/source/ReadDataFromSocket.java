package com.xgsama.flink.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ReadDataFromSocket
 *
 * @author : xgSama
 * @date : 2022/1/12 10:46:16
 */
public class ReadDataFromSocket {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketSource1 = env.socketTextStream("127.0.0.1", 8809);
        socketSource1.print("socketSource1 ");

        DataStreamSource<String> socketSource2 = env.socketTextStream("127.0.0.1", 8809, "\n");
        socketSource2.print("socketSource2 ");


        DataStreamSource<String> socketSource3 = env.socketTextStream("127.0.0.1", 8809, "\n", 10);
        socketSource3.print("socketSource3 ");



        env.execute();
    }
}
