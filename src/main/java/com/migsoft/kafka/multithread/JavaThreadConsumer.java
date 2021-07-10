package com.migsoft.kafka.multithread;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

public class JavaThreadConsumer extends Thread{

    private static final Logger log = LoggerFactory.getLogger(JavaThreadConsumer.class);

    private final KafkaConsumer<String, String> consumer;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    public JavaThreadConsumer(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void run() {
        consumer.subscribe(Arrays.asList("java-topic"));
        while (!closed.get()) {
            try {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> consumerRecord: consumerRecords) {
                    log.info("Offset = {}, Partition = {}, Key = {}, Value = {}", consumerRecord.offset(), consumerRecord.partition(),
                            consumerRecord.key(), consumerRecord.value());
                }
            } catch (WakeupException wex) {
                if(!closed.get()) {
                    throw wex;
                }
            } finally {
                consumer.close();
            }
        }
    }

    public void shutDown() {
        closed.set(true);
        consumer.wakeup();
    }
}
