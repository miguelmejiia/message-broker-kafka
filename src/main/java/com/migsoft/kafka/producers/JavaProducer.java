package com.migsoft.kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class JavaProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Broker to connect
        props.put("acks", "all"); // 0 - 1 - all
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // key type (IntegerSerializer (ex))
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // key type

        try (Producer<String, String> producer = new KafkaProducer<>(props)){
            producer.send(new ProducerRecord<>("java-topic", "java-key", "java-value"));
        } catch (Exception e){

        }

    }
}
