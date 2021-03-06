package com.xgsama.java;

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
        String path = "fences.dat";

        InputStream resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);

        ObjectInputStream objectInputStream = new ObjectInputStream(resourceAsStream);

        HashMap hashMap = (HashMap) objectInputStream.readObject();

        System.out.println();
    }
}
