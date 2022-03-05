package com.xgsama.flink.state;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;

/**
 * RocksDBStateBackendDemo
 *
 * @author : xgSama
 * @date : 2022/2/25 10:06:06
 */
public class RocksDBStateBackendDemo {
    public static void main(String[] args) throws Exception {
        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend("");
    }
}
