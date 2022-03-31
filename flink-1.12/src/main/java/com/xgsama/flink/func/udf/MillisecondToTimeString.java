package com.xgsama.flink.func.udf;

import org.apache.flink.table.functions.ScalarFunction;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * LongToTimeString
 *
 * @author : xgSama
 * @date : 2021/8/30 15:16:08
 */
public class MillisecondToTimeString extends ScalarFunction {

    public String eval(Long millisecond) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.ofInstant(Instant.ofEpochMilli(millisecond), ZoneId.of("Asia/Shanghai")));
    }

    public String eval(Long millisecond, String pattern) {
        return DateTimeFormatter.ofPattern(pattern).format(LocalDateTime.ofInstant(Instant.ofEpochMilli(millisecond), ZoneId.of("Asia/Shanghai")));
    }

    public static void main(String[] args) {
        MillisecondToTimeString millisecondToTimeString = new MillisecondToTimeString();
        System.out.println(millisecondToTimeString.eval(6852395801914774000L, "yyyyMMdd"));
        System.out.println(millisecondToTimeString.eval(1634659200000L));
    }
}
