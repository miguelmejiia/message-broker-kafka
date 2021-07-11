package com.migsoft.kafka.callbacks;

import com.migsoft.kafka.producers.JavaProducer;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class CallbackProducer {
    public static final Logger log = LoggerFactory.getLogger(CallbackProducer.class);

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("linger.ms", "5");

        try (Producer<String, String> producer = new KafkaProducer<>(props)){
            for (int i = 0; i<10000; i++) {
                producer.send(new ProducerRecord<>("java-topic", String.valueOf(i), "java-value"),
                   new Callback() {
                    // Callback - Executes when the message is delivered
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e != null) {
                            log.info("There was an error {} ", e.getMessage());
                        }
                        log.info("Offset = {}, Partition {}, Topic {} ", recordMetadata.offset(), recordMetadata.partition(), recordMetadata.topic());
                    }
                });
            }
            producer.flush();
        }
        log.info("Processing time = {} ms", (System.currentTimeMillis() - startTime));

    }
}
