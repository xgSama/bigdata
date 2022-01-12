package com.xgsama.flink.udf;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * UDTFTest
 *
 * @author : xgSama
 * @date : 2021/12/7 16:24:26
 */
public class UDTFTest extends TableFunction<Row> {
    @Override
    public TypeInformation<Row> getResultType() {
        return new RowTypeInfo(Types.STRING, Types.INT);
    }

    public void eval(String str) {
        String[] split = str.split("@");
        Row row = new Row(2);
        row.setField(0, split[0]);
        row.setField(1, split[0].length());
        collect(row);
    }
}
