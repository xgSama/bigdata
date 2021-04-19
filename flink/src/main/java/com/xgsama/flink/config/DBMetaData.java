package com.xgsama.flink.config;

/**
 * DBMetaData
 *
 * @author xgSama
 * @date 2021/4/12 18:01
 */
public class DBMetaData {

    private String url;
    private String user;
    private String password;
    private int age;

    public DBMetaData(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}
