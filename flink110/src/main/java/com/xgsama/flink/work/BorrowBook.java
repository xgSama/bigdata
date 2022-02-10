package com.xgsama.flink.work;

import com.alibaba.fastjson.JSONObject;
import com.mysql.jdbc.Driver;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;

/**
 * BorrowBook
 *
 * @author : xgSama
 * @date : 2021/12/7 14:04:03
 */
public class BorrowBook {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.16.100.109:9092");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "earliest");


        DataStreamSource<Tuple4<Long, Long, Long, Long>> recordSource =
                env.addSource(new FlinkKafkaConsumer<>(
                        "bigdatadev_book",
                        new AbstractDeserializationSchema<Tuple4<Long, Long, Long, Long>>() {
                            @Override
                            public Tuple4<Long, Long, Long, Long> deserialize(byte[] bytes) throws IOException {
                                BorrowRecord value = JSONObject.parseObject(bytes, BorrowRecord.class);
                                return new Tuple4<>(value.id, value.user_id, value.book_id, value.browse);
                            }
                        }, properties));

        SingleOutputStreamOperator<Tuple4<Long, Long, Long, Long>> borrow =
                recordSource
                        .keyBy(2)
                        .sum(3);

        String url = "jdbc:mysql://172.16.23.23:3306/test?autoReconnect=true&useSSL=false";
        String user = "drpeco";
        String password = "DT@Stack#123";
        String sql = "select book_id,book_name,book_price,book_storage from test_book";

        borrow
                .flatMap(new RichFlatMapFunction<Tuple4<Long, Long, Long, Long>, Row>() {

                    private Connection conn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        Class.forName(Driver.class.getName());

                        this.conn = DriverManager.getConnection(url, user, password);
                    }

                    @SuppressWarnings({"SqlResolve", "SqlNoDataSourceInspection"})
                    @Override
                    public void flatMap(Tuple4<Long, Long, Long, Long> value, Collector<Row> out) throws Exception {
                        String bookId = value.f2.toString();
                        String sql = "select book_id,book_name,book_price,book_storage from test_book where book_id=" + bookId + ";";
                        ResultSet resultSet = conn.prepareStatement(sql).executeQuery();
                        Row row = new Row(3);
                        row.setField(0, value.f2);
                        row.setField(2, value.f3);
                        while (resultSet.next()) {
                            row.setField(1, resultSet.getString("book_name"));
                            out.collect(row);
                        }
                    }

                    @Override
                    public void close() throws Exception {
                        this.conn.close();
                    }
                })
                .print("result: ");

//        RowTypeInfo rowTypeInfo = new RowTypeInfo(
//                BasicTypeInfo.INT_TYPE_INFO,
//                BasicTypeInfo.STRING_TYPE_INFO,
//                BasicTypeInfo.DOUBLE_TYPE_INFO,
//                BasicTypeInfo.INT_TYPE_INFO
//        );
//
//
//        DataStreamSource<Row> source = JdbcConnectorUtil.addSource(env, url, sql, rowTypeInfo, user, password);
//        source.print("mysql: ");
//
//        DataStream<Tuple3<Long, String, Long>> apply = borrow
//                .join(source)
//                .where(new KeySelector<Tuple4<Long, Long, Long, Long>, Long>() {
//                    @Override
//                    public Long getKey(Tuple4<Long, Long, Long, Long> value) throws Exception {
//                        return value.f2;
//                    }
//                })
//                .equalTo(new KeySelector<Row, Long>() {
//                    @Override
//                    public Long getKey(Row value) throws Exception {
//                        return Long.valueOf((Integer) value.getField(0));
//                    }
//                })
//                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
//                .apply(new FlatJoinFunction<Tuple4<Long, Long, Long, Long>, Row, Tuple3<Long, String, Long>>() {
//                    @Override
//                    public void join(Tuple4<Long, Long, Long, Long> first, Row second, Collector<Tuple3<Long, String, Long>> out) throws Exception {
//                        out.collect(new Tuple3<>(first.f2, (String) second.getField(1), first.f3));
//                    }
//
//                });
//
//        apply.print("result: ");
        env.execute();
    }


    static class BorrowRecord {
        // "id":1,"user_id":1001,"book_id":101,"browse":3
        private long id;
        private long user_id;
        private long book_id;
        private long browse;

        public BorrowRecord() {
        }

        public BorrowRecord(long id, long user_id, long book_id, long browse) {
            this.id = id;
            this.user_id = user_id;
            this.book_id = book_id;
            this.browse = browse;
        }

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public long getUser_id() {
            return user_id;
        }

        public void setUser_id(long user_id) {
            this.user_id = user_id;
        }

        public long getBook_id() {
            return book_id;
        }

        public void setBook_id(long book_id) {
            this.book_id = book_id;
        }

        public long getBrowse() {
            return browse;
        }

        public void setBrowse(long browse) {
            this.browse = browse;
        }

        @Override
        public String toString() {
            return "BorrowRecord{" +
                    "id=" + id +
                    ", user_id=" + user_id +
                    ", book_id=" + book_id +
                    ", browse=" + browse +
                    '}';
        }
    }

}
