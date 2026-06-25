package com.oracle.exercice3.clickconsumer.kafka;


import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

@Service
public class ClickStore {

    private final Map<String, Long> counts =
            new ConcurrentHashMap<>();

    public void update(String user,
                       Long count) {

        counts.put(user, count);
    }

    public Map<String, Long> getCounts() {
        return counts;
    }
}