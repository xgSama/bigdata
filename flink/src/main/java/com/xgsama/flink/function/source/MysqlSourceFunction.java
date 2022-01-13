package com.xgsama.flink.function.source;

import com.xgsama.flink.model.Student;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * MysqlSourceFunction
 *
 * @author : xgSama
 * @date : 2022/1/12 22:44:23
 */
public class MysqlSourceFunction implements SourceFunction<Student> {

    private volatile boolean isRunning = true;

    private PreparedStatement ps;
    private Connection connection;


    @SuppressWarnings("all")
    private void init() {
        String jdbcUrl = "jdbc:mysql://xgsama:3307/test?useSSL=false";
        String user = "root";
        String password = "cyz19980815";

        String sql = "select * from student;";
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection(jdbcUrl, user, password);
            ps = connection.prepareStatement(sql);
            ps.setFetchSize(100);
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = " + e.getMessage());
        }
    }

    @Override
    public void run(SourceContext<Student> ctx) throws Exception {
        init();


        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            Student student = new Student();
            student.setId(resultSet.getInt("id"));
            student.setUsername(resultSet.getString("name").trim());
            student.setGender(resultSet.getString("gender").trim());
            student.setAge(resultSet.getInt("age"));

            ctx.collect(student);
        }

    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }

}
