package com.xgsama.flink.function.source;

import com.xgsama.flink.model.Student;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * MysqlParallelSourceFunction
 *
 * @author : xgSama
 * @date : 2022/1/13 14:33:59
 */
@Slf4j
public class MysqlParallelSourceFunction extends RichParallelSourceFunction<Student> {

    private volatile boolean isRunning = true;

    private PreparedStatement ps;
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {

        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        int parallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();

        int start = indexOfThisSubtask * 4;
        int end = 4 * indexOfThisSubtask + 3;

        @SuppressWarnings("all")
        String sql = "select * from student where id >= " + start + " and id <= " + end + ";";
        log.info("\nindexOfThisSubtask: {}\nparallelSubtasks: {}\nexecuteQuery: {}",
                indexOfThisSubtask, parallelSubtasks, sql);

        connection = getConnection();
        ps = connection.prepareStatement(sql);
        ps.setFetchSize(100);

    }

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

    }

    private Connection getConnection() {
        String jdbcUrl = "jdbc:mysql://xgsama:3307/test?useSSL=false";
        String user = "root";
        String password = "cyz19980815";
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection(jdbcUrl, user, password);
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = " + e.getMessage());
        }
        return con;
    }


}
