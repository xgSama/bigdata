package com.xgsama.spark.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * MyUDF
 *
 * @author : xgSama
 * @date : 2021/8/27 10:06:48
 */
public class MyUDF extends UDF {

    public String evaluate(String a, String b) {
        return a + b;
    }

}
