package com.xgsama.flink.model;

import com.github.javafaker.Faker;
import jdk.nashorn.internal.objects.annotations.Constructor;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

/**
 * Student
 *
 * @author xgSama
 * @date 2021/4/19 11:20
 */
@Data
@NoArgsConstructor
public class Student implements Serializable {
    private int id;
    private String username;
    private String password;
    private int status;

    public Student(int id, String username, String password, int status) {
        this.id = id;
        this.username = username;
        this.password = password;
        this.status = status;
    }

    private static Faker faker = Faker.instance(Locale.CHINA);

    public static Student randomStudent() {
        int id = faker.random().nextInt(999);
        String name = faker.name().name();
        String password = UUID.randomUUID().toString().substring(0, 6);
        int status = faker.number().numberBetween(0, 2);
        return new Student(id, name, password, status);
    }

    public static List<Student> newStudent(int num) {

        List<Student> list = new ArrayList<>();

        for (int i = 0; i < num; i++) {
            list.add(randomStudent());
        }
        return list;
    }
}
