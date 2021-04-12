package com.xgsama.java.util;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * KafkaUtil
 *
 * @author xgSama
 * @date 2020/11/5 10:19
 */
public class KafkaUtil {
    public static void main(String[] args) throws IOException {
        FileInputStream inputStream = new FileInputStream("input/sensor.txt");

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));

        System.out.println(bufferedReader.readLine().toString());
    }

}
