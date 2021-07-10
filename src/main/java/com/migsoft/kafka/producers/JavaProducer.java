package com.migsoft.kafka.producers;

import java.util.Properties;

public class JavaProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Broker to connect
        props.put("acks", "1"); // 0 - 1 - all
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // key type (IntegerSerializer (ex))
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // key type
    }
}
