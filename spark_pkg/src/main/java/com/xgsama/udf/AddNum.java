package com.xgsama.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * AddNum
 *
 * @author : xgSama
 * @date : 2021/9/1 13:53:31
 */
public class AddNum extends UDF {
    public int evaluate(int a, int b) {
        return a + b;
    }
}
