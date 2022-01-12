package com.xgsama.flink.util;

import com.xgsama.flink.entity.User;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * MysqlSink
 *
 * @author : xgSama
 * @date : 2021/9/23 14:40:20
 */
@SuppressWarnings("SqlNoDataSourceInspection")
public class MysqlSink extends RichSinkFunction<List<User>> {

    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://rm-bp10661g217i4ze99io.mysql.rds.aliyuncs.com:3306/testforuser?useSSL=false";

    private PreparedStatement ps;
    private Connection connection;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName(JDBC_DRIVER);
        connection = DriverManager.getConnection(DB_URL, "rds_test", "Testforuser2021");
        String sql = "insert into zy_test_user(id,name,age) values (?, ?, ?);";
        ps = connection.prepareStatement(sql);
    }

    @Override
    public void invoke(List<User> users, Context context) throws Exception {
        for (User user : users) {
            ps.setInt(1, user.getId());
            ps.setString(2, user.getName());
            ps.setInt(3, user.getAge());
            ps.addBatch();
        }

        //一次性写入
        int[] count = ps.executeBatch();
        System.out.println("成功写入Mysql数量：" + count.length);

    }


    @Override
    public void close() throws Exception {
        super.close();
        //关闭并释放资源
        if (connection != null) {
            connection.close();
        }

        if (ps != null) {
            ps.close();
        }
    }
}
