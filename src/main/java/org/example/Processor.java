package org.example;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TableJoined;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

public class Processor {


    public Processor() {
    }

    public void process(StreamsBuilder builder) {

        var punctuationInterval = Duration.ofSeconds(10);
        var maxAgeDuration = Duration.ofDays(0);

        final Serde<String> stringSerde = Serdes.String();

        KTable<String, String> trackingStatesKTable = builder
                .stream("leftTopic", Consumed.with(stringSerde, stringSerde))
                .toTable(Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("leftTopicTable")
                        .withKeySerde(stringSerde)
                        .withValueSerde(stringSerde));


        KTable<String, String> orgDebitorKTable = builder
                .stream("rightTopic", Consumed.with(stringSerde, stringSerde))
                .toTable(Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("rightTopicTable")
                        .withKeySerde(stringSerde)
                        .withValueSerde(stringSerde));


        KTable<String, String> joinTable = trackingStatesKTable.leftJoin(orgDebitorKTable, s -> "x",
                (value1, value2) -> "y",
                TableJoined.as("join"),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("joinTable")
                        .withKeySerde(stringSerde)
                        .withValueSerde(stringSerde));

        trackingStatesKTable
                .toStream()
                .processValues(() -> new org.example.TimestampAwareStoreCleaner<>(punctuationInterval, maxAgeDuration, "leftTopicTable", 30000L), "leftTopicTable")
                .foreach((k, v) -> {});

    }

}