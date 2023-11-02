package com.example.pfa.kafka;

import com.example.pfa.config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executors;

@Service
public class CsvkafkaProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(CsvkafkaProducer.class);

    private KafkaTemplate<String, String> kafkaTemplate;
    private AppConfig appConfig;

    public CsvkafkaProducer(KafkaTemplate<String, String> kafkaTemplate, AppConfig appConfig) {
        this.kafkaTemplate = kafkaTemplate;
        this.appConfig = appConfig;

        // Start file monitoring in a separate thread
        Executors.newSingleThreadExecutor().execute(this::monitorCsvFile);
    }


    private void monitorCsvFile() {
        Path csvFilePath = Paths.get(appConfig.getCsvFilePath());
        long lastModified = csvFilePath.toFile().lastModified();

        while (true) {
            try {
                Thread.sleep(5000); // Adjust this interval as needed
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.error("Thread interrupted: ", e);
                return;
            }

            long currentModified = csvFilePath.toFile().lastModified();

            if (currentModified > lastModified) {
                LOGGER.info("CSV file has been modified. Sending new data to Kafka.");
                lastModified = currentModified; // Update the last modified timestamp
                sendNewDataToKafka();
            }
        }
    }

    public void sendNewDataToKafka() {
        try (BufferedReader reader = new BufferedReader(new FileReader(appConfig.getCsvFilePath()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                kafkaTemplate.send("churn", line);
            }
        } catch (IOException e) {
            LOGGER.error("Error reading CSV file:", e);
        }
    }

}

