package com.oracle.exercice2;

public class StationStats {

    double totalTemp = 0;
    double totalHumidity = 0;
    int count = 0;

    public StationStats add(double temp,
                            double humidity) {

        totalTemp += temp;
        totalHumidity += humidity;
        count++;

        return this;
    }

    public double avgTemp() {
        return count == 0 ? 0 : totalTemp / count;
    }

    public double avgHumidity() {
        return count == 0 ? 0 : totalHumidity / count;
    }
}