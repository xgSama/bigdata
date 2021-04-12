package com.xgsama.java.util;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * TimestampUtil
 *
 * @author xgSama
 * @date 2020/11/9 17:47
 */
public class TimestampUtil {

    public static Long getSeconds() {
        return Instant.now().getEpochSecond();
    }

    public static Long getMilliseconds() {
        return Instant.now().toEpochMilli();
    }

    public static String getStringDate(Long milliseconds) {
        // to Instant
        Instant ins = Instant.ofEpochMilli(milliseconds);
        ZonedDateTime zdt = ZonedDateTime.ofInstant(ins, ZoneId.of("UTC+8"));

        return zdt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    public static String getStringDate(Long milliseconds, String formatter) {
        // to Instant
        Instant ins = Instant.ofEpochMilli(milliseconds);
        ZonedDateTime zdt = ZonedDateTime.ofInstant(ins, ZoneId.of("UTC+8"));

        return zdt.format(DateTimeFormatter.ofPattern(formatter));
    }

    public static Long timeToMilliseconds(String time) {
        return null;
    }
}
