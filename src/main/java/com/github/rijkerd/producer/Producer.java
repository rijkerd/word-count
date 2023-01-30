package com.github.rijkerd.producer;

import com.github.javafaker.Faker;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.stream.Stream;

@Component
@RequiredArgsConstructor
public class Producer {

    private final KafkaTemplate<Integer,String> template;
    Faker faker;

    public final String Topic = "word-topic-in";


    @EventListener(ApplicationStartedEvent.class)
    public void generate() {

        faker = Faker.instance();

        final Flux<Long> interval = Flux.interval(Duration.ofMillis(2_000));

        final Flux<String> quotes = Flux.fromStream(Stream.generate(() -> faker.hobbit().quote()));

        Flux.zip(interval, quotes)
                .map(it -> template.send(Topic, faker.random().nextInt(42), it.getT2())).blockLast();
    }
}
