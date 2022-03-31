package com.xgsama.flink.util;

import com.xgsama.flink.entity.User;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.*;

/**
 * EnvUtil
 *
 * @author: xgsama
 * @date: 2022/3/31 20:15:40
 */
public class EnvUtil {

    public static String crateTable(StreamTableEnvironment tableEnv) {

        Table table = tableEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.STRING()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("age", DataTypes.INT()),
                        DataTypes.FIELD("height", DataTypes.DOUBLE()),
                        DataTypes.FIELD("birthday", DataTypes.TIMESTAMP())
                ),

                row("1001", "haha", 18, 111.11, new Timestamp(Instant.now().toEpochMilli())),
                row("1002", "lala", 22, 178.11, new Timestamp(Instant.now().toEpochMilli() + 2000L))
        );

        tableEnv.createTemporaryView("student", table);


        return "student";
    }


    public static String crateTable2(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {

        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> sourceStream = env
                .readTextFile("input/sensor.txt")
                .map(new MapFunction<String, Tuple3<String, Long, Integer>>() {
                    @Override
                    public Tuple3<String, Long, Integer> map(String s) throws Exception {
                        String[] split = s.split(",");

                        return new Tuple3<>(split[0].trim(), Long.valueOf(split[1].trim()), Integer.valueOf(split[2].trim()));
                    }
                }, TypeInformation.of(new TypeHint<Tuple3<String, Long, Integer>>() {
                }));


        tableEnv.createTemporaryView("s_table", sourceStream, $("s_id"), $("s_time"), $("s_temp"), $("pt").proctime());

        Table tableResult = tableEnv.sqlQuery("select * from s_table");

        tableEnv.createTemporaryView("t2", tableResult);
        tableEnv.executeSql("select * from t2").print();

        return "user";
    }


}
