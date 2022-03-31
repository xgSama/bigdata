package com.xgsama.flink.func.udaf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

/**
 * MaxFunction
 *
 * @author: xgsama
 * @date: 2022/3/30 22:13:17
 */
public class MaxFunction extends AggregateFunction<Object, Row> {

    @Override
    public Row createAccumulator() {
        return new Row(1);
    }

    @FunctionHint(
            accumulator = @DataTypeHint("ROW<max BIGINT>"),
            output = @DataTypeHint("BIGINT"))
    public void accumulate(Row accumulator, Long l) {
        final Long max = (Long) accumulator.getField(0);
        if (max == null || l > max) {
            accumulator.setField(0, l);
        }
    }

    @FunctionHint(
            accumulator = @DataTypeHint("ROW<max STRING>"),
            output = @DataTypeHint("STRING"))
    public void accumulate(Row accumulator, String s) {
        final String max = (String) accumulator.getField(0);
        if (max == null || s.compareTo(max) > 0) {
            accumulator.setField(0, s);
        }
    }

    @Override
    public Object getValue(Row accumulator) {
        return accumulator.getField(0);
    }


}
