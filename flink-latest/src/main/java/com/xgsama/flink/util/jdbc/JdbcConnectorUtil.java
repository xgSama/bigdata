package com.xgsama.flink.util.jdbc;

import com.xgsama.flink.config.DBMetaData;
import com.xgsama.flink.util.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.types.Row;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.getInfoFor;

/**
 * JdbcConnectorUtil
 *
 * @author xgSama
 * @date 2021/4/12 17:26
 */
public class JdbcConnectorUtil {

    public static DataStreamSource<Row> addSource(StreamExecutionEnvironment env,
                                                  String url, String table,
                                                  String user, String password, Class<?> clazz) {

        String sql = createSql(clazz, table);
        // 2、生成RowTypeInfo
        RowTypeInfo rowTypeInfo = createRowTypeInfo(clazz);

        return addSource(env, url, sql, rowTypeInfo, user, password);

    }

    public static DataStreamSource<Row> addSource(StreamExecutionEnvironment env,
                                                  String url, String sql, RowTypeInfo rowTypeInfo,
                                                  String user, String password) {


        JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setRowTypeInfo(rowTypeInfo)
                .setQuery(sql)
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl(url)
                .setUsername(user)
                .setPassword(password)
                .finish();

        env.addSource(new InputFormatSourceFunction<>(jdbcInputFormat, TypeExtractor.getInputFormatTypes(jdbcInputFormat)));

        return env.createInput(jdbcInputFormat);

    }

    @SuppressWarnings("rawtypes")
    private static RowTypeInfo createRowTypeInfo(Class<?> clazz) {

        Field[] fields = clazz.getDeclaredFields();

        int n = fields.length;
        TypeInformation[] fieldTypes = new TypeInformation[n];

        for (int i = 0; i < n; i++) {
            fieldTypes[i] = getInfoFor(fields[i].getType());
        }


        return new RowTypeInfo(fieldTypes);
    }

    public static String createSql(Class<?> clazz, String table) {
        StringBuilder sb = new StringBuilder();
        Field[] fields = clazz.getDeclaredFields();
        sb.append("select ");
        for (Field field : fields) {
            sb.append(field.getName().toLowerCase()).append(",");
        }
        sb.deleteCharAt(sb.length() - 1)
                .append(" ")
                .append("from ")
                .append(table)
                .append(";");

        return sb.toString();

    }


    public static <T> void addSink(SingleOutputStreamOperator<T> data, String sql, String dbType, String url, String user, String password) {

        Class<T> typeClass = data.getType().getTypeClass();

        Field[] declaredFields = typeClass.getDeclaredFields();

        data.addSink(JdbcSink.sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {

                        Class<?> aClass = t.getClass();

                        Field[] fields = aClass.getDeclaredFields();

                        for (Field field : fields) {

                        }

                    }
                }, new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(url)
                        .withDriverName(getDriverName(dbType))
                        .withUsername(user)
                        .withPassword(password)
                        .build()
        ));
    }

    public static String getDriverName(String dbType) {
        switch (dbType.toLowerCase()) {
            case "mysql":
                return "com.mysql.jdbc.Driver";
            case "oracle":
                return "oracle.jdbc.driver.OracleDriver";
            default:
                throw new RuntimeException("error dbType");
        }
    }


//    private static Map<String, TypeInformation> map = new HashMap<String, TypeInformation>() {{
//        put("java.lang.String", BasicTypeInfo.STRING_TYPE_INFO);
//    }}

    public static void main(String[] args) throws NoSuchMethodException {

        System.out.println(createSql(DBMetaData.class, "TEST"));

        DBMetaData o = new DBMetaData("url", "user", "pwd");


        Class<? extends DBMetaData> aClass = o.getClass();
        Field[] fields = aClass.getDeclaredFields();


        for (Field field : fields) {
            System.out.println(field);
            System.out.println(field.getType());

            String name = field.getType().getName();

            System.out.println(getInfoFor(field.getType()));

            System.out.println(field.getType().getName());
            Method declaredMethod = aClass.getDeclaredMethod("get" + StringUtils.firstChar2Upper(field.getName()));
            try {
                Object invoke = declaredMethod.invoke(o);
                System.out.println(invoke);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }


    }

}
