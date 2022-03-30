package com.xgsama.flink.model;

import com.mysql.jdbc.Driver;
import lombok.*;

import java.io.Serializable;

/**
 * JobConf
 *
 * @author : xgSama
 * @date : 2022/1/10 17:31:31
 */
@Builder
@ToString
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SourceJobConf implements Serializable {

    private String driver;
    private String dbUrl;
    private String user;
    private String password;
    private String splitKey;
    private String tableName;
    private String whereFilter;
}
