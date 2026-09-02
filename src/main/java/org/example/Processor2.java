package org.example;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

public class Processor2 {


    public Processor2() {
    }

    public void process(StreamsBuilder builder) {

        var punctuationInterval = Duration.ofSeconds(30);
        var maxAgeDuration = Duration.ofDays(1);

        final Serde<String> stringSerde = Serdes.String();

        KTable<String, String> trackingStatesKTable = builder
                .stream("leftTopic", Consumed.with(stringSerde, stringSerde))
                .toTable(Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("leftTopicTable")
                        .withKeySerde(stringSerde)
                        .withValueSerde(stringSerde));



        trackingStatesKTable
                .toStream()
                .processValues(() -> new TimestampAwareStoreCleaner<>(punctuationInterval, maxAgeDuration, "leftTopicTable", 30000L), "leftTopicTable")
                .foreach((k, v) -> {});

        trackingStatesKTable.toStream().to("outputTopic", Produced.with(stringSerde, stringSerde));

    }

}