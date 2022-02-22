package com.xgsama.flink.iceberg.table.hdfs;

import com.xgsama.flink.util.EnvUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

/**
 * ReadWithDataStream
 *
 * @author : xgSama
 * @date : 2022/2/15 10:41:43
 */
public class WriteWithDataStream {
    public static void main(String[] args) throws Exception {
        EnvUtil.setHadoopUserName("admin");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Configuration conf = EnvUtil.getDtTestConfiguration();

        TableLoader tableLoader = TableLoader.fromHadoopTable(
                "/iceberg/default_database/all_users_sink"
                , conf
        );

        GenericRowData rowData = GenericRowData.of(1, new BinaryStringData("iceberg"), 12);
        DataStream<RowData> input = env.fromElements(rowData);

        FlinkSink.forRowData(input)
                .tableLoader(tableLoader)
                .overwrite(false)
                .append();


        env.execute();

    }
}
