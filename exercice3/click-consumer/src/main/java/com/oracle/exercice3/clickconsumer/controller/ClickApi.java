package com.oracle.exercice3.clickconsumer.controller;

import com.oracle.exercice3.clickconsumer.kafka.ClickStore;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class ClickApi {

    private final ClickStore store;

    public ClickApi(ClickStore store) {
        this.store = store;
    }

    @GetMapping("/clicks/count")
    public Map<String, Long> getCounts() {

        return store.getCounts();
    }
}