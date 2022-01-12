package com.xgsama.java.test;

import com.github.javafaker.Faker;
import com.mysql.jdbc.Driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Locale;

/**
 * CreateData
 *
 * @author xgSama
 * @date 2020/11/12 18:46
 */
@SuppressWarnings({"SqlResolve", "SqlNoDataSourceInspection"})
public class CreateData {
    public static void main(String[] args) throws Exception {
        int count = 999;

        Faker faker = new Faker(Locale.CHINA);
        Class.forName(Driver.class.getName());

        Connection connection = DriverManager.getConnection(
                "jdbc:mysql://xgsama:3307/test?useSSL=false&useUnicode=true&characterEncoding=UTF-8", "root", "cyz19980815");
        connection.setAutoCommit(false);
        String insertSql = "insert into single_table(key1, key2, key3, key_part1, key_part2, key_part3, common_field) \n" +
                "values (?,?,?,?,?,?,?);";

        PreparedStatement preparedStatement = connection.prepareStatement(insertSql);

        while (count >= 0) {
            preparedStatement.setString(1, faker.name().firstName());
            preparedStatement.setInt(2, count);
            preparedStatement.setString(3, faker.name().lastName());
            preparedStatement.setString(4, faker.company().name());
            preparedStatement.setString(5, faker.color().name());
            preparedStatement.setString(6, faker.phoneNumber().cellPhone());
            preparedStatement.setString(7, faker.address().city());
            preparedStatement.addBatch();
            if (count % 100 == 0) {
                preparedStatement.executeBatch();
                preparedStatement.clearBatch();
                System.out.println("100");
            }
            count--;
        }

        int[] ints = preparedStatement.executeBatch();
        for (int anInt : ints) {
            System.out.print(anInt + "  ");
        }
        connection.commit();
        connection.close();


    }

}
