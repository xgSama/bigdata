package com.xgsama.flink.func.udf;

import org.apache.flink.table.functions.ScalarFunction;

import java.util.UUID;

/**
 * GenerateUUID
 *
 * @author: xgsama
 * @date: 2022/4/1 21:16:16
 */
public class GenerateUUID extends ScalarFunction {
    public String eval() {
        return UUID.randomUUID().toString();
    }
}
