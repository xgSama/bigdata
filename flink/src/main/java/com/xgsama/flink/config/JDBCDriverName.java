package com.xgsama.flink.config;

/**
 * JDBCDriverName
 *
 * @author xgSama
 * @date 2021/2/24 11:21
 */
public enum JDBCDriverName {
    MYSQL5("com.mysql.jdbc.Driver"),
    MYSQL6("com.mysql.cj.jdbc.Driver"),
    ORACLE("oracle.jdbc.driver.OracleDriver");

    private final String driverName;

    JDBCDriverName(String driverName) {
        this.driverName = driverName;
    }

    public String driver() {
        return driverName;
    }

}
