package com.oracle.exercice3.clickstreams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@EnableKafkaStreams
public class StreamConfig {

    @Bean
    public KStream<String, String> process(
            StreamsBuilder builder) {

        KStream<String, String> stream =
                builder.stream(
                        "clicks",
                        Consumed.with(
                                Serdes.String(),
                                Serdes.String()
                        )
                );

        stream
                .groupByKey(
                        Grouped.with(
                                Serdes.String(),
                                Serdes.String()
                        )
                )
                .count()
                .toStream()
                .mapValues(Object::toString)
                .to(
                        "click-counts",
                        Produced.with(
                                Serdes.String(),
                                Serdes.String()
                        )
                );

        return stream;
    }
}