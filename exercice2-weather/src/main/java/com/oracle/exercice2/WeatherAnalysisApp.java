package com.oracle.exercice2;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class WeatherAnalysisApp {

    private static final Map<String, StationStats> statsMap =
            new HashMap<>();

    public static void main(String[] args) {

        Properties props = new Properties();

        props.put(
                StreamsConfig.APPLICATION_ID_CONFIG,
                "weather-analysis-app"
        );

        props.put(
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );

        props.put(
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass()
        );

        props.put(
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass()
        );

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> weatherData =
                builder.stream("weather-data");

        KStream<String, String> results =
                weatherData.flatMap((key, value) -> {

                    try {

                        String[] parts = value.split(",");

                        if (parts.length != 3)
                            return java.util.Collections.emptyList();

                        String station =
                                parts[0].trim();

                        double tempC =
                                Double.parseDouble(
                                        parts[1].trim()
                                );

                        double humidity =
                                Double.parseDouble(
                                        parts[2].trim()
                                );

                        // Filtre température > 30°C

                        if (tempC <= 30)
                            return java.util.Collections.emptyList();

                        // Conversion Fahrenheit

                        double tempF =
                                (tempC * 9.0 / 5.0)
                                        + 32.0;

                        StationStats stats =
                                statsMap.computeIfAbsent(
                                        station,
                                        s -> new StationStats()
                                );

                        stats.add(
                                tempF,
                                humidity
                        );

                        String result =
                                station
                                        + " : Temperature moyenne = "
                                        + String.format(
                                        "%.1f",
                                        stats.avgTemp()
                                )
                                        + " F , Humidite moyenne = "
                                        + String.format(
                                        "%.1f",
                                        stats.avgHumidity()
                                )
                                        + " %";

                        System.out.println(result);

                        return java.util.List.of(
                                KeyValue.pair(
                                        station,
                                        result
                                )
                        );

                    } catch (Exception e) {

                        System.out.println(
                                "Message invalide : "
                                        + value
                        );

                        return java.util.Collections.emptyList();
                    }
                });

        results.to(
                "station-averages",
                Produced.with(
                        Serdes.String(),
                        Serdes.String()
                )
        );

        KafkaStreams streams =
                new KafkaStreams(
                        builder.build(),
                        props
                );

        streams.start();

        System.out.println(
                "WeatherAnalysisApp started..."
        );

        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(streams::close)
                );
    }
}