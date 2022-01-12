package com.xgsama.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * UDFTest
 *
 * @author : xgSama
 * @date : 2021/12/7 15:45:26
 */
public class UDFTest extends ScalarFunction {
    public String eval(String subject) {

        int length = subject == null ? 0 : subject.length();

        if (length % 2 == 0) {
            return " even_dtstack-----" + subject;
        } else {
            return " odd_dtstack-----" + subject;
        }
    }

}
