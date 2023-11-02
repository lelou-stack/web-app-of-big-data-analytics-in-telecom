package com.example.pfa.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AppConfig {

    @Value("${csv.file.path}")
    private String csvFilePath;

    public String getCsvFilePath() {
        return csvFilePath;
    }
}

