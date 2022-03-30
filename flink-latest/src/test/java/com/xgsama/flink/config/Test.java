package com.xgsama.flink.config;

/**
 * Test
 *
 * @author xgSama
 * @date 2021/2/24 11:26
 */
public class Test {

    public static void main(String[] args) {
        System.out.println(JDBCDriverName.MYSQL5.driver());

        for (JDBCDriverName value : JDBCDriverName.values()) {
            System.out.println(value);
            System.out.print(value.ordinal() + "\t");
            System.out.print(value.name() + "\t");
            System.out.print(value.driver());
            System.out.println();
        }
    }
}
