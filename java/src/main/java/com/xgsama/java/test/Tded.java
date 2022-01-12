package com.xgsama.java.test;

import java.util.concurrent.atomic.LongAdder;

/**
 * Tded
 *
 * @author : xgSama
 * @date : 2021/11/3 16:08:06
 */
public class Tded {
    public static void main(String[] args) {
        LongAdder longAdder = new LongAdder();
        longAdder.add(2L);
        System.out.println(longAdder.longValue());
    }
}
