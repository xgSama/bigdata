package com.xgsama.flink.function.source;

import com.xgsama.flink.model.Student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * MysqlSourceRichFunction
 *
 * @author : xgSama
 * @date : 2022/1/13 10:34:09
 */
public class MysqlSourceRichFunction extends RichSourceFunction<Student> {

    private volatile boolean isRunning = true;

    private PreparedStatement ps;
    private Connection connection;

    // 函数的初始化方法。 它在实际工作方法（如map或join ）之前调用，因此适合一次性设置工作。
    // 对于作为迭代一部分的函数，此方法将在每个iteration superstep 开始时调用。
    // 传递给函数的配置对象可用于配置和初始化。 配置包含在程序组合中的功能上配置的所有参数。
    @Override
    @SuppressWarnings("all")
    public void open(Configuration parameters) throws Exception {
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

    // 启动源。 实现可以使用SourceFunction.SourceContext发出元素
    @Override
    public void run(SourceContext<Student> ctx) throws Exception {
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

    // 用户代码的拆卸方法。 在最后一次调用主要工作方法（例如map或join ）之后调用它。 对于作为迭代一部分的函数，此方法将在每次迭代超级步之后调用。
    // 此方法可用于清理工作
    @Override
    public void close() throws Exception {
        ps.close();
        connection.close();
    }
}
