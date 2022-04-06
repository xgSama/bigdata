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
public class UDFTestP {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        tableEnv.getConfig().getConfiguration().setString("python.files", "/tmp/py_func/f.py");
        tableEnv.getConfig().getConfiguration().setString("python.executable", "python3");
        tableEnv.getConfig().getConfiguration().setString("python.client.executable", "python3");


        tableEnv.executeSql("create temporary function calc_len as 'f.calc_len' language python");


        String sql = "select calc_len('abv')";
        Table table = tableEnv.sqlQuery(sql);
        tableEnv.toAppendStream(table, Row.class).print("udf");

        env.execute(UDFTestP.class.getSimpleName());
    }
}
