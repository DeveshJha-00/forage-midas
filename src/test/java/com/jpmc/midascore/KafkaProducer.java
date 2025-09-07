package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducer {
    private final String topic;
    private final KafkaTemplate<String, Transaction> kafkaTemplate;

    public KafkaProducer(@Value("${general.kafka-topic}") String topic, KafkaTemplate<String, Transaction> kafkaTemplate) {
        this.topic = topic;
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(String transactionLine) {
        //transactionLine = "6, 7, 122.86"
        String[] transactionData = transactionLine.split("\n");

        kafkaTemplate.send(topic, new Transaction(Long.parseLong(transactionData[0].split(", ")[0]), Long.parseLong(transactionData[0].split(", ")[1]), Float.parseFloat(transactionData[0].split(", ")[2])));
    }
}