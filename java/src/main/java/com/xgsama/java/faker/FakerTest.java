package com.xgsama.java.faker;

import com.github.javafaker.Faker;
import com.xgsama.java.util.TimeUtil;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * FakerTest
 *
 * @author xgSama
 * @date 2021/4/12 10:20
 */
public class FakerTest {

    public static void main(String[] args) {
        Faker faker = new Faker(Locale.CHINA);

        Calendar instance = Calendar.getInstance(TimeZone.getDefault(), Locale.CHINA);

        System.out.println(Date.from(TimeUtil.add(instance, 3).atZone(ZoneId.systemDefault()).toInstant()));

        for (int i = 0; i < 10; i++) {
            System.out.println(faker.company().name());
        }


        for (int i = 0; i < 10; i++) {
            System.out.print("sensor_" + faker.number().numberBetween(1, 9) + ",");
            System.out.print(faker.number().randomDouble(2, 30, 70) + ",");
            System.out.print(faker.date().between(new Date(), Date.from(TimeUtil.add(instance, 3).atZone(ZoneId.systemDefault()).toInstant())));
            System.out.println();
        }
    }
}
