package com.xgsama.flink.func.udtf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;

/**
 * OverloadedFunction
 *
 * @author: xgsama
 * @date: 2022/3/31 19:50:07
 */
@FunctionHint(
        input = {@DataTypeHint("STRING"), @DataTypeHint("INT")},
        output = @DataTypeHint("STRING")
)
@FunctionHint(
        input = {@DataTypeHint("BIGINT"), @DataTypeHint("BIGINT")},
        output = @DataTypeHint("BIGINT")
)
@FunctionHint(
        input = {},
        output = @DataTypeHint("BOOLEAN")
)
public class OverloadedFunction extends TableFunction<Object> {
    public void eval(Object... o) {
        if (o.length == 0) {
            collect(false);
        } else {
            collect(o[0]);
        }
    }
}
