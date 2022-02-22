package com.xgsama.flink.iceberg;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;

/**
 * ReadIcebergWithDataStream
 *
 * @author : xgSama
 * @date : 2022/2/12 23:03:28
 */
public class ReadIcebergWithDataStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        TableLoader tableLoader = TableLoader.fromHadoopTable("file:///tmp/iceberg/warehouse/default_database/ds_users_sink");

        DataStream<RowData> icebergSource = FlinkSource.forRowData()
                .env(env)
                .tableLoader(tableLoader)
                // false：Batch Read
                .streaming(true)
                .build();

        icebergSource
                .filter((FilterFunction<RowData>) value -> value.getInt(2) == 14)
                .map((MapFunction<RowData, String>) value -> value.getInt(0) + ":" + value.getString(1) + ":" + value.getInt(2))
                .print();

        env.execute("Test Iceberg Batch Read");
    }
}
