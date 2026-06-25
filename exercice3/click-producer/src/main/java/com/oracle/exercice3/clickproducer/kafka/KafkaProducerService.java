package com.oracle.exercice3.clickproducer.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * @author {ANAS DR}
 **/
@Service
public class KafkaProducerService {

    private final KafkaTemplate<String,String> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String,String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendClick(String userId) {

        kafkaTemplate.send(
                "clicks",
                userId,
                "click"
        );
    }
}
