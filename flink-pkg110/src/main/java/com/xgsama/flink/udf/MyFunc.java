package com.xgsama.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * MyFunc
 *
 * @author : xgSama
 * @date : 2021/8/30 14:43:04
 */
public class MyFunc extends ScalarFunction {
    public long eval(String a) {
        return a == null ? 0 : a.length();
    }


    public String eval(String b, String c) {
        return b + c;
    }
}
