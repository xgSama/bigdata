package com.xgsama.java.util;

import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * ProducerTest
 *
 * @author xgSama
 * @date 2020/11/5 11:28
 */
public class ProducerTest {
    private KafkaProducerUtil producer;

    @Before
    public void init() {
        producer = new KafkaProducerUtil("172.16.101.50:9092");
    }

    @Test
    public void testSend() {

//        System.out.println(producer.props.getProperty("buffer.memory"));
//        System.out.println(producer.props.toString());

        producer.sendOneMessage("zheye", "{\"temperature\":\"40\",\"id\":\"sensor_t\",\"timestamp\":\"1547718230001\"}");
    }
    @Test
    public void sendReader() throws IOException {
        FileInputStream inputStream = new FileInputStream("F:\\JAVA\\bigdata\\input\\sensor.txt");
        producer.sendMessage(inputStream, "zheye", ",", "id", "timestamp", "temperature");

    }

    @Test
    public void sendReader1() throws IOException {
        FileInputStream inputStream = new FileInputStream("F:\\JAVA\\bigdata\\input\\test.txt");
        producer.sendMessage(inputStream, "test2");

    }

    public static void main(String[] args) throws IOException {
         KafkaProducerUtil producer = new KafkaProducerUtil("172.16.101.50:9092");
        FileInputStream inputStream = new FileInputStream("input/sensor.txt");
        producer.sendMessage(inputStream, "zheye", ",", "id", "timestamp", "temperature");
    }
}
