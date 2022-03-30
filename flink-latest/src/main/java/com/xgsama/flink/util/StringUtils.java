package com.xgsama.flink.util;

/**
 * StringUtils
 *
 * @author xgSama
 * @date 2021/4/7 16:14
 */
public class StringUtils {

    public static String firstChar2Upper(String str) {
        if (str != null && str.length() > 0) {
            char[] chars = str.toCharArray();
            chars[0] = Character.toUpperCase(chars[0]);
            return new String(chars);
        }

        return str;
    }
}
