package com.xgsama.flink.flinkx;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * JdbcInputFormat
 *
 * @author xgSama
 * @date 2021/1/29 14:04
 */
public class JdbcInputFormat extends BaseRichInputFormat {

    public static final int resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
    public static int resultSetType = ResultSet.TYPE_FORWARD_ONLY;
//    public DatabaseInterface databaseInterface;

    public String table;
    public String queryTemplate;
    public String customSql;
    public String querySql;

    public String splitKey;
    public int fetchSize;
    public int queryTimeOut;
//    public IncrementConfig incrementConfig;

    public String username;
    public String password;
    public String driverName;
    public String dbUrl;
    public Properties properties;
    public transient Connection dbConn;
    public transient Statement statement;
    public transient PreparedStatement ps;
    public transient ResultSet resultSet;
    public boolean hasNext;

/*
    public List<MetaColumn> metaColumns;
    public List<String> columnTypeList;
    public int columnCount;
    public MetaColumn restoreColumn;
    public Row lastRow = null;

    //for postgre
    public TypeConverterInterface typeConverter;

    public int numPartitions;

    public StringAccumulator maxValueAccumulator;
    public BigIntegerMaximum endLocationAccumulator;
    public BigIntegerMaximum startLocationAccumulator;



    //轮询增量标识字段类型
    public ColumnType type;
    */

    //The hadoop config for metric
    public Map<String, Object> hadoopConfig;

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {

    }
}
