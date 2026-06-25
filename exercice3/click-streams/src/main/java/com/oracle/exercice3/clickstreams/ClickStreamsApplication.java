package com.oracle.exercice3.clickstreams;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class ClickStreamsApplication {

    public static void main(String[] args) {
        SpringApplication.run(ClickStreamsApplication.class, args);
    }

}
