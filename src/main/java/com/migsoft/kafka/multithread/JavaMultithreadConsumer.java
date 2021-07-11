package com.migsoft.kafka.multithread;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class JavaMultithreadConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "course-group");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        ExecutorService executor = Executors.newFixedThreadPool(5);

        for (int i = 0; i < 5; i++) {
            JavaThreadConsumer consumer = new JavaThreadConsumer(new KafkaConsumer<>(props));
            executor.execute(consumer);
        }
        while (!executor.isTerminated()) ;
    }
}
