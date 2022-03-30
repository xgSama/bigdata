package com.xgsama.flink.source;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.types.Row;

/**
 * ReadDataFromFile
 *
 * @author : xgSama
 * @date : 2022/1/12 10:28:07
 */
public class ReadDataFromFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

//        String filePath = "/Users/xgSama/IdeaProjects/bigdata/input/sensor.txt";
//
//        DataStreamSource<String> fileSource1 = env.readTextFile(filePath, "utf8");
//        fileSource1.print();

        String csvFilePath = "/Users/xgSama/IdeaProjects/bigdata/input/csv";
        TypeInformation<?>[] types = {
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO
        };


        FileInputFormat<Row> rowCsvInputFormat = new RowCsvInputFormat(new Path(csvFilePath), types, "\r\n", "|");
        DataStreamSource<Row> source = env.readFile(rowCsvInputFormat, csvFilePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 10000);
        source.setParallelism(1);

        source.print();

        env.execute();
    }
}
