package com.oracle.exercice1;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class TextCleaningApp {

    private static final List<String> FORBIDDEN_WORDS = List.of("HACK", "SPAM", "XXX");

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "text-cleaning-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> rawMessages = builder.stream("text-input");

        // Nettoyage : trim + espaces multiples + majuscules
        KStream<String, String> cleaned = rawMessages.mapValues(value -> {
            if (value == null) return "";
            String trimmed = value.trim();
            String singleSpaced = trimmed.replaceAll("\\s+", " ");
            return singleSpaced.toUpperCase();
        });

        // Branching : valides vs invalides
        Map<String, KStream<String, String>> branches = cleaned.split(Named.as("branch-"))
                .branch((key, value) -> isValid(value), Branched.as("valid"))
                .defaultBranch(Branched.as("invalid"));

        branches.get("branch-valid").to("text-clean");
        branches.get("branch-invalid").mapValues(v -> "Message rejeté").to("text-dead-letter");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static boolean isValid(String message) {
        if (message == null || message.isBlank()) return false;
        if (message.length() > 100) return false;
        for (String word : FORBIDDEN_WORDS) {
            if (message.contains(word)) return false; // message déjà en majuscules
        }
        return true;
    }
}