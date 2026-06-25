package com.oracle.exercice3.clickconsumer.kafka;


import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ClickConsumer {

    private final ClickStore store;

    public ClickConsumer(ClickStore store) {
        this.store = store;
    }

    @KafkaListener(
            topics = "click-counts",
            groupId = "click-api")
    public void consume(
            String value,
            org.apache.kafka.clients.consumer.ConsumerRecord<String,String> record) {

        store.update(
                record.key(),
                Long.parseLong(value)
        );
    }
}
