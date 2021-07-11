package com.migsoft.kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class JavaProducer {

    public static final Logger log = LoggerFactory.getLogger(JavaProducer.class);

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Broker to connect
        props.put("acks", "all"); // 0 - 1 - all
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // key type (IntegerSerializer (ex))
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // value type
        props.put("linger.ms", "5"); // key type

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 100; i++) {
                producer.send(new ProducerRecord<>("java-topic", String.valueOf(i), "java-value"));
            }
            producer.flush();
        }
        // 3652 ms too send 1,000,000 messages
        log.info("Processing time = {} ms", (System.currentTimeMillis() - startTime));

    }
}
