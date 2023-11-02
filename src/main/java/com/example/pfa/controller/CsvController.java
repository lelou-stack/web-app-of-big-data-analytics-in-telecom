package com.example.pfa.controller;

import com.example.pfa.kafka.CsvkafkaProducer;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/csv")
public class CsvController {
    private final CsvkafkaProducer csvkafkaProducer;

    public CsvController(CsvkafkaProducer csvkafkaProducer) {
        this.csvkafkaProducer = csvkafkaProducer;
    }


    @PostMapping("/upload")
    public ResponseEntity<String> uploadCsvFile() {
        try {
            csvkafkaProducer.sendNewDataToKafka();


            return ResponseEntity.ok("CSV file uploaded .");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("An error occurred during CSV uploading.");
        }
    }


}