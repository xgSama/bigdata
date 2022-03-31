package com.xgsama.flink.udf;

import com.xgsama.flink.func.udaf.CountUDAF;
import com.xgsama.flink.func.udf.MillisecondToTimeString;
import com.xgsama.flink.func.udtf.SplitUDTF;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * UDFTest
 *
 * @author : xgSama
 * @date : 2021/10/19 10:50:25
 */
public class UDFTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


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
        tableEnv.createFunction("to_time_string", MillisecondToTimeString.class);
        tableEnv.createFunction("temp_count", CountUDAF.class);
        tableEnv.createFunction("split_name", SplitUDTF.class);

        String sql = "select s_id, to_time_string(s_time) s_time, s_temp from s_table";
        Table table = tableEnv.sqlQuery(sql);
        tableEnv.toAppendStream(table, Row.class).print("udf");

        String sql2 = "select s_id, temp_count(s_temp) total from s_table group by s_id";
        Table table2 = tableEnv.sqlQuery(sql2);
        tableEnv.toRetractStream(table2, Row.class).print("udaf");

        String sql3 = "select s_id,c1,c2 from s_table, lateral table(split_name(s_id)) as t(c1,c2)";
        Table table3 = tableEnv.sqlQuery(sql3);
        tableEnv.toAppendStream(table3, Row.class).print("udtf");

        env.execute(UDFTest.class.getSimpleName());
    }
}
