package com.xgsama.flink.iceberg;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

/**
 * ReadIcebergWithDataStream
 *
 * @author : xgSama
 * @date : 2022/2/12 23:03:28
 */
public class WriteIcebergWithDataStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        TableLoader tableLoader = TableLoader.fromHadoopTable("file:///tmp/iceberg/warehouse/default_database/ds_users_sink");


        GenericRowData rowData = GenericRowData.of(1, new BinaryStringData("iceberg"), 12);

        DataStream<RowData> input = env.fromElements(rowData);

        input.print();

        DataStreamSink<Void> build = FlinkSink
                .forRowData(input)
                .tableLoader(tableLoader)
                .overwrite(false)
                .append();

        env.execute("Test Iceberg Batch Read");
    }
}
