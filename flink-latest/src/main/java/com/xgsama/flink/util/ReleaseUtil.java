package com.xgsama.flink.util;

import java.io.Closeable;
import java.io.IOException;

/**
 * ReleaseUtil
 *
 * @author : xgSama
 * @date : 2022/1/13 10:05:37
 */
public class ReleaseUtil {

    public static void release(Closeable... closeables) {

        for (Closeable closeable : closeables) {
            if (closeable != null) {
                try {
                    closeable.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
