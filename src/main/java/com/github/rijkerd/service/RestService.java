package com.github.rijkerd.service;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class RestService {
    private final StreamsBuilderFactoryBean factoryBean;

    @GetMapping("/count/{word}")
    public ResponseEntity getCount(@PathVariable String word){

        final KafkaStreams kafkaStreams =  factoryBean.getKafkaStreams();
        final ReadOnlyKeyValueStore<String, Long> counts = kafkaStreams.store(StoreQueryParameters
                .fromNameAndType("counts", QueryableStoreTypes.keyValueStore()));

        return ResponseEntity.ok(counts.get(word));
    }
}
