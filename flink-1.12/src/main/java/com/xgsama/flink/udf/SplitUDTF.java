package com.xgsama.flink.udf;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.types.Row;

import java.util.Optional;

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


//    @Override
//    public TypeInformation<Row> getResultType() {
//        return new RowTypeInfo(Types.STRING, Types.STRING);
//    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("col1", DataTypes.STRING()),
                        DataTypes.FIELD("clo2", DataTypes.STRING())
                );

        return TypeInference.newBuilder()
                .outputTypeStrategy(callContext -> Optional.of(dataType))
                .build();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
