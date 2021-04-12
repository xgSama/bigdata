package com.xgsama.flink.connector;

import com.mysql.jdbc.Driver;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * JDBCReader
 *
 * @author xgSama
 * @date 2021/2/23 17:30
 */
public class JDBCReader extends RichSourceFunction<Tuple2<String,String>> {

    private static final Logger logger = LoggerFactory.getLogger(JDBCReader.class);

    private final Connection connection = null;
    private final PreparedStatement ps = null;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        DriverManager.registerDriver(new Driver());



    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
