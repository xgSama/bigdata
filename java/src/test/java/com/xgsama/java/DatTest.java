package com.xgsama.java;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.HashMap;

/**
 * DatTest
 *
 * @author : xgSama
 * @date : 2022/3/9 16:06:28
 */
public class DatTest {

    public static void main(String[] args) throws Exception {
        String path = "/Users/xgSama/IdeaProjects/bigdata/java/src/main/resources/fences.dat";

        InputStream resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);

        ObjectInputStream objectInputStream = new ObjectInputStream(resourceAsStream);

        HashMap hashMap = (HashMap) objectInputStream.readObject();

        System.out.println();
    }
}
