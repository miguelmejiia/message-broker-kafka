package com.migsoft.kafka.callbacks;

import com.migsoft.kafka.producers.JavaProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class CallbackProducer {
    public static final Logger log = LoggerFactory.getLogger(JavaProducer.class);

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("linger.ms", "5");

        try (Producer<String, String> producer = new KafkaProducer<>(props)){
            for (int i = 0; i<1000000; i++) {
                producer.send(new ProducerRecord<>("java-topic", String.valueOf(i), "java-value"));
            }
            producer.flush();
        }
        log.info("Processing time = {} ms", (System.currentTimeMillis() - startTime));

    }
}
