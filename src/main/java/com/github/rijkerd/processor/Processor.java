package com.github.rijkerd.processor;


import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class Processor {

    private String STREAM_INPUT_TOPIC = "word-topic-in";

    private String STREAM_OUT_TOPIC = "word-topic-out";

    @Autowired
    public void process(StreamsBuilder builder) {

        final Serde<Integer> integerSerde = Serdes.Integer();
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        KStream<Integer, String> textLines = builder.stream(STREAM_INPUT_TOPIC, Consumed.with(integerSerde, stringSerde));

        KTable<String, Long> wordCounts = textLines
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                .groupBy((key, value) -> value, Grouped.with(stringSerde, stringSerde))
                .count(Materialized.as("counts"));

        wordCounts.toStream().to(STREAM_OUT_TOPIC, Produced.with(stringSerde, longSerde));
    }
}
