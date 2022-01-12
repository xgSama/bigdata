package com.xgsama.java.util;

import java.sql.DriverManager;

/**
 * JDBCUtil
 *
 * @author : xgSama
 * @date : 2021/9/7 15:27:16
 */
public class JDBCUtil {
    public static void main(String[] args) throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/imooc", "root", "root");
    }
}
