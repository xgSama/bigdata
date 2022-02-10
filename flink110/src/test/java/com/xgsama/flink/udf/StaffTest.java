package com.xgsama.flink.udf;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * StaffTest
 *
 * @author : xgSama
 * @date : 2022/1/5 13:52:52
 */
public class StaffTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);


        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        env.setParallelism(1);

        SingleOutputStreamOperator<String> sourceStream = env
                .readTextFile("input/staff.txt");

        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO);

        SingleOutputStreamOperator<Row> map = sourceStream.map(new MapFunction<String, Row>() {

            @Override
            public Row map(String value) throws Exception {

                Row row = new Row(12);
                StaffAmt o = JSONObject.parseObject(value, StaffAmt.class);

                row.setField(0, o.getPtfloSno());
                row.setField(1, o.getAppDate());
                row.setField(2, o.getOrdAmt());
                row.setField(3, o.getPtfloOpType());
                row.setField(4, o.getIaInvestAgrNo());
                row.setField(5, o.getCancelFlag());
                row.setField(6, o.getPtfloOrdStat());
                row.setField(7, o.getOpertion());
                row.setField(8, o.getComrate());
                row.setField(9, o.getStaffno());
                row.setField(10, o.getCurScale());
                row.setField(11, o.getTimeS());
                return row;
            }
        }, rowTypeInfo);


        sourceStream.print();

        tableEnv.createTemporaryView("sourcess", map, "ptfloSno, appDate, ordAmt, ptfloOpType, iaInvestAgrNo, cancelFlag, ptfloOrdStat," +
                "opertion, comrate, staffno, curScale, timeS");
        tableEnv.registerFunction("staff_func", new StaffCountUDAF());

        // {"ptfloSno":"001","appDate":"20220104","ordAmt":"10000","ptfloOpType":"0","iaInvestAgrNo":" ",
        // "cancelFlag":"0","ptfloOrdStat":"1","opertion":"INSERT","comrate":"50","staffno":"csc10001",
        // "curScale":"5000","timeS":"2022-01-04T10:00:00.000"}
        String sql = "select staff_func(ptfloSno, appDate, ordAmt, ptfloOpType, iaInvestAgrNo, cancelFlag, ptfloOrdStat," +
                "opertion, comrate, staffno, curScale, timeS) as f from sourcess";

        Table table = tableEnv.sqlQuery(sql);

        tableEnv.toRetractStream(table, Row.class).print();

        env.execute();

    }
}
