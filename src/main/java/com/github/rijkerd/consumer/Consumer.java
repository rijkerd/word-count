package com.github.rijkerd.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class Consumer {
    @KafkaListener(topics = {"word-topic-out"}, groupId = "spring-consumer")
    public void consume(ConsumerRecord<String, Long> record) {
        log.info("Word {} - Count: {}",record.key(),record.value());
    }
}
