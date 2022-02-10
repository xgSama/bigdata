package com.xgsama.flink.udf;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * JsonUDTFTest
 *
 * @author : xgSama
 * @date : 2021/12/2 14:15:13
 */
public class JsonUDTFTest {

    @Test
    public void testEval() throws Exception {

        BufferedReader reader = new BufferedReader(new FileReader("/Users/xgSama/IdeaProjects/bigdata/input/zxjt.txt"));

        String s = reader.readLine();

        new JsonUDTF().eval(s);

        System.out.println(s);
    }


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);


        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        env.setParallelism(1);

        SingleOutputStreamOperator<String> sourceStream = env
                .readTextFile("input/zxjt.txt");


        sourceStream.print();


        tableEnv.createTemporaryView("sourcess", sourceStream, "sources");
        tableEnv.registerFunction("json_func", new JsonUDTF());

        String sql =
                "select\n" +
                        "        d.itemId ,\n" +
                        "        d.isDup ,\n" +
                        "        d.copyright ,\n" +
                        "        d.title ,\n" +
                        "        d.publishTime ,\n" +
                        "        d.source1 as source ,\n" +
                        "        case \n" +
                        "            when d.operationType='insesourcesrt' then 'i'       \n" +
                        "            when  d.operationType='update' then 'u'       \n" +
                        "            when d.operationType='delete' then 'd' \n" +
                        "        end as operationType ,\n" +
                        "        d.category ,\n" +
                        "        d.tagCode ,\n" +
                        "        d.tagName ,\n" +
                        "        d.source2 as tagSource ,\n" +
                        "        d.level ,\n" +
                        "        d.weight ,\n" +
                        "        d.emotion ,\n" +
                        "        d.emotionWeight ,\n" +
                        "        d.tagSystems ,\n" +
                        "        d.updateTime   \n" +
                        "    from\n" +
                        "        sourcess u,\n" +
                        "        lateral table(json_func(sources)) as d(itemId ,\n" +
                        "        isDup ,\n" +
                        "        copyright ,\n" +
                        "        title ,\n" +
                        "        publishTime ,\n" +
                        "        source1 ,\n" +
                        "        operationType ,\n" +
                        "        category      ,\n" +
                        "        tagCode       ,\n" +
                        "        tagName       ,\n" +
                        "        source2        ,\n" +
                        "        level         ,\n" +
                        "        weight        ,\n" +
                        "        emotion       ,\n" +
                        "        emotionWeight ,\n" +
                        "        tagSystems,\n" +
                        "        updateTime)  \n" +
                        "        where " +
                        "        isDup = '0' \n" +
                        "        and copyright = '1' \n" +
                        "        and title is not null";


        Table table = tableEnv.sqlQuery(sql);

        tableEnv.toAppendStream(table, Row.class).print();

        env.execute();
    }
}
