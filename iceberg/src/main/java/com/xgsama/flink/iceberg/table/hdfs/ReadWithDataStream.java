package com.xgsama.flink.iceberg.table.hdfs;

import com.xgsama.flink.util.EnvUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;

/**
 * ReadWithDataStream
 *
 * @author : xgSama
 * @date : 2022/2/15 10:41:43
 */
public class ReadWithDataStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Configuration conf = EnvUtil.getDtTestConfiguration();

        TableLoader tableLoader = TableLoader.fromHadoopTable(
                "/iceberg/default_database/all_users_sink"
                , conf
        );

        DataStream<RowData> icebergSource = FlinkSource.forRowData()
                .env(env)
                .tableLoader(tableLoader)
                // false：Batch Read
                .streaming(true)
                .build();

        icebergSource.print();

        env.execute();

    }
}
