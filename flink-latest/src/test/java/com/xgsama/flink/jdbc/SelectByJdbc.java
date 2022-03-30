package com.xgsama.flink.jdbc;

import com.mysql.jdbc.Driver;

import java.sql.*;

/**
 * SelectByJdbc
 *
 * @author : xgSama
 * @date : 2022/1/4 16:51:29
 */
@SuppressWarnings({"SqlResolve", "SqlNoDataSourceInspection"})
public class SelectByJdbc {
    public static void main(String[] args) throws Exception {

        Class.forName("com.mysql.jdbc.Driver");
        Connection connection = DriverManager.getConnection(
                "jdbc:mysql://rm-bp10661g217i4ze99io.mysql.rds.aliyuncs.com:3306/testforuser?useSSL=false",
                "rds_test",
                "Testforuser2021");
        connection.setAutoCommit(false);
        PreparedStatement ps = connection.prepareStatement("select * from zy_sync_flinkx_source");

        ps.setFetchSize(1);

        ResultSet resultSet = ps.executeQuery();

        while (resultSet.next()) {
            int id = resultSet.getInt(1);
            String name = resultSet.getString(2);
            int age = resultSet.getInt(3);
            Timestamp birthday = resultSet.getTimestamp(4);
            System.out.println(id + "\t" + name + "\t" + age + "\t" + birthday);
        }
    }
}
