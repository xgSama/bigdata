package com.xgsama.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Student
 *
 * @author xgSama
 * @date 2021/4/19 11:20
 */
@Data
@AllArgsConstructor
public class Student {
    private int id;
    private String username;
    private String password;
    private int status;
}
