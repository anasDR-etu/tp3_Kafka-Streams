package com.oracle.exercice3.clickproducer.controller;

import com.oracle.exercice3.clickproducer.kafka.KafkaProducerService;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author {ANAS DR}
 **/
@Controller
public class ClickController {

    private final KafkaProducerService producer;

    public ClickController(KafkaProducerService producer) {
        this.producer = producer;
    }

    @GetMapping("/")
    public String home() {
        return "index";
    }

    @GetMapping("/click")
    public String click() {

        producer.sendClick("user1");

        return "index";
    }

}
