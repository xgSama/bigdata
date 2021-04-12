package com.xgsama.java.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Future;

/**
 * KafkaProducerUtil
 *
 * @author xgSama
 * @date 2020/11/5 10:39
 */
public class KafkaProducerUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerUtil.class);

    private long localCacheSize = 100L;
    public Properties props = new Properties();
    private KafkaProducer<String, String> producer;
    private List<ProducerRecord<String, String>> kafkaData = null;

    private KafkaProducerUtil() {
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 33554432);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 335544320);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    }


    public KafkaProducerUtil(String serverIp) {
        this();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverIp);

        producer = new KafkaProducer<>(props);
        kafkaData = Collections.synchronizedList(new ArrayList<ProducerRecord<String, String>>());
    }


    /**
     * set Local Cache Size.
     *
     * @param size Local Cache Size
     * @return this
     */
    public KafkaProducerUtil setLocalCacheSize(long size) {
        this.localCacheSize = size;
        return this;
    }

    /**
     * 发送一条信息
     *
     * @param topic 要发送的topic
     * @param msg   要发送的信息
     */
    public void sendOneMessage(String topic, String msg) {

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, msg);
        try {
            Future<RecordMetadata> send = producer.send(record);
            // 如果future调用get()，则将阻塞，直到相关请求完成并返回该消息的metadata，或抛出发送异常。
            // future.get();
            System.out.println(record.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.flush();
    }

    /**
     * Send a set number of Local Cache to Kafka Broker.
     */
    @SuppressWarnings("SynchronizeOnNonFinalField")
    public void sendLocalCache() {
        synchronized (kafkaData) {
            kafkaData.forEach(msg -> {
                producer.send(msg);
            });
            producer.flush();
            kafkaData.clear();
        }
    }

    public void sendMessage(InputStream inputStream, String topic) throws IOException {

        String line;
        Map<String, String> map = new HashMap<>();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));

        while ((line = bufferedReader.readLine()) != null) {

            sendOneMessage(topic, line);
        }
    }

    public void sendMessage(InputStream inputStream, String topic, String regex, String... fields) throws IOException {

        String line;
        Map<String, String> map = new HashMap<>();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));

        while ((line = bufferedReader.readLine()) != null) {
            String[] split = line.split(regex);

            for (int i = 0; i < fields.length; i++) {
                map.put(fields[i], split[i].trim());
            }

            String string = JSONObject.toJSONString(map);

            sendOneMessage(topic, string);
        }
    }

}
