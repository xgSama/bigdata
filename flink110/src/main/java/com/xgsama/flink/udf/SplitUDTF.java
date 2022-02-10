package com.xgsama.flink.udf;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * SplitUDTF
 *
 * @author : xgSama
 * @date : 2021/10/19 10:41:07
 */
public class SplitUDTF extends TableFunction<Row> {

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
    }

    public void eval(String str) {
        String[] split = str.split("_");
        String first = split[0];
        String second = split[1];
        Row row = new Row(2);
        row.setField(0, first);
        row.setField(1, second);
        collect(row);
    }


    @Override
    public TypeInformation<Row> getResultType() {
        return new RowTypeInfo(Types.STRING, Types.STRING);
    }


    @Override
    public void close() throws Exception {
        super.close();
    }
}
