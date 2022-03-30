package com.xgsama.flink.lookup;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * MyConfigOptions
 *
 * @author: xgsama
 * @date: 2022/3/21 21:34:30
 */
public class MyConfigOptions {
    public static final String IDENTIFIER = "my-connector";

    public static final ConfigOption<String> URL = ConfigOptions.key("url").stringType().noDefaultValue().withDescription("JDBC URL");
    public static final ConfigOption<String> USERNAME = ConfigOptions.key("username").stringType().defaultValue("root").withDescription("username");
    public static final ConfigOption<String> PASSWORD = ConfigOptions.key("password").stringType().noDefaultValue().withDescription("password");

    public static final ConfigOption<String> DATABASE = ConfigOptions.key("database").stringType().noDefaultValue().withDescription("连接的数据库");
    public static final ConfigOption<String> TABLE = ConfigOptions.key("table").stringType().noDefaultValue().withDescription("连接的表名");



}
