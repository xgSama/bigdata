package com.xgsama.java.util;

import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;


/**
 * TSTest
 *
 * @author xgSama
 * @date 2020/11/10 19:18
 */
public class TSTest {
    @Test
    public void getUnix() {
        System.out.println(TimeUtil.getMilliseconds());
    }

    @Test
    public void unix2Date() {
        // to Instant
        Instant ins = Instant.ofEpochMilli(TimeUtil.getMilliseconds());
        ZonedDateTime zdt = ZonedDateTime.ofInstant(ins, ZoneId.of("UTC+8"));


        System.out.println(zdt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
    }

    @Test
    public void test1() {
        ZonedDateTime zdt = ZonedDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm ZZZZ");
        System.out.println(formatter.format(zdt));

        DateTimeFormatter zhFormatter = DateTimeFormatter.ofPattern("yyyy MMM dd EE HH:mm", Locale.CHINA);
        System.out.println(zhFormatter.format(zdt));

        DateTimeFormatter usFormatter = DateTimeFormatter.ofPattern("E, MMMM/dd/yyyy HH:mm", Locale.US);
        System.out.println(usFormatter.format(zdt));
    }

    @Test
    public void test() {
//        System.out.println(TimestampUtil.getMilliseconds());

        System.out.println(TimeUtil.getStringDate(1605595020589L));

//        System.out.println(Instant.now().getEpochSecond());
//        System.out.println(Instant.now().toEpochMilli());
    }
}


