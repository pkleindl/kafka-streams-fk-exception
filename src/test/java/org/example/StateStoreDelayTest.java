package org.example;


import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class StateStoreDelayTest {

    @Container
    static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1")).withExposedPorts(9092);
    private static KafkaStreams streams;

    static AtomicReference<Throwable> uncaught = new AtomicReference<>();
    static CountDownLatch errorLatch = new CountDownLatch(1);

    private static Logger logger = LoggerFactory.getLogger(StateStoreDelayTest.class);

    @BeforeAll
    static void setup() {

        StreamsBuilder builder = new StreamsBuilder();
        new Processor2().process(builder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fk-join-invalid-partition-it-" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(StreamsConfig.STATE_DIR_CONFIG, createTempStateDir());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        AdminClient adminClient = AdminClient.create(props);
        List<NewTopic> topics = List.of(new NewTopic("leftTopic",6, (short) 1),
                new NewTopic("outputTopic",6, (short) 1)
                );
        adminClient.createTopics(topics);

        streams = new KafkaStreams(builder.build(), props);
        streams.setUncaughtExceptionHandler(exception -> {
            uncaught.set(exception);
            errorLatch.countDown();
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        });

        streams.start();
    }

    @AfterAll
    static void tearDown() {
        if (streams != null) {
            streams.close(Duration.ofSeconds(10));
        }
    }

    @Test
    void shouldSurfaceInvalidPartitionMinusOneAfterCleanerEvictionAndFkUpdate() throws Exception {
        while (!KafkaStreams.State.RUNNING.equals(streams.state())) {
            System.out.println("Current state: " + streams.state());
            Thread.sleep(Duration.ofSeconds(3));
        }

        System.out.println("Current state: " + streams.state());

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", KAFKA.getBootstrapServers());
        producerProps.put("acks", "all");
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());

        //long oldTs = System.currentTimeMillis() - Duration.ofDays(2).toMillis();
        System.out.println(KAFKA.getBootstrapServers());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            for (int i = 1; i <= 360; i++) {
            // Left side tracking state
            producer.send(new ProducerRecord<>(
                    "leftTopic",
                    null,
                    System.currentTimeMillis(),
                    "someKey-"+ i,
                    "someValue-" + i + " " + Date.from(Instant.now())
            )).get();

            //producer.flush();
            logger.info("Produced message: {}", i);
            Thread.sleep(Duration.ofSeconds(1).toMillis());
            }
        }

        logger.info("Finished, waiting");
        Thread.sleep(Duration.ofSeconds(600).toMillis());
    }

    private static String createTempStateDir() {
        try {
            return Files.createTempDirectory("ks-state-").toFile().getAbsolutePath();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String stackTraceToString(Throwable t) {
        StringBuilder sb = new StringBuilder();
        while (t != null) {
            sb.append(t).append('\n');
            for (StackTraceElement ste : t.getStackTrace()) {
                sb.append("\tat ").append(ste).append('\n');
            }
            t = t.getCause();
            if (t != null) {
                sb.append("Caused by: ");
            }
        }
        return sb.toString();
    }

}
