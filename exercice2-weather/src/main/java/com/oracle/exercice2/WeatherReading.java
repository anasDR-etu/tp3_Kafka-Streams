package com.oracle.exercice2;

public class WeatherReading {

    String station;
    double temperature;
    double humidity;

    public WeatherReading(String station,
                          double temperature,
                          double humidity) {
        this.station = station;
        this.temperature = temperature;
        this.humidity = humidity;
    }
}