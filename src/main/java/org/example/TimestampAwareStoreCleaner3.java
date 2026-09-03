package org.example;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.function.BiPredicate;

public class TimestampAwareStoreCleaner3<K, V> implements FixedKeyProcessor<K, V, V> {

    private static final Logger log = LoggerFactory.getLogger(TimestampAwareStoreCleaner3.class);

    public TimestampAwareStoreCleaner3(Duration punctuateInterval,
                                       Duration maxAge,
                                       String storeName) {
        this(punctuateInterval, maxAge, storeName, PunctuationType.WALL_CLOCK_TIME, Long.MAX_VALUE);
    }

    public TimestampAwareStoreCleaner3(Duration punctuateInterval,
                                       Duration maxAge,
                                       String storeName,
                                       Long maxPunctuateMs) {
        this(punctuateInterval, maxAge, storeName, PunctuationType.WALL_CLOCK_TIME, maxPunctuateMs);
    }

    public TimestampAwareStoreCleaner3(Duration punctuateInterval,
                                       Duration maxAge,
                                       String storeName,
                                       PunctuationType punctuationType,
                                       Long maxPunctuateMs) {
        this.punctuateInterval = punctuateInterval;
        this.deleteIfTrue = (rts, currentTimestamp) -> {

            var maximumTimeInPast = Instant.ofEpochMilli(currentTimestamp).minus(maxAge);
            var recordTimestamp = Instant.ofEpochMilli(rts);
            return maximumTimeInPast.isAfter(recordTimestamp);
        };
        this.maxAge = maxAge;
        this.storeName = storeName;
        this.punctuationType = punctuationType;
        this.maxPunctuateMs = maxPunctuateMs;

    }

    private final Duration punctuateInterval;
    private final Duration maxAge;
    private final BiPredicate<Long, Long> deleteIfTrue;
    private final String storeName;
    private final PunctuationType punctuationType;

    private final Long maxPunctuateMs;

    private FixedKeyProcessorContext<K, V> context;
    private TimestampedKeyValueStore<Object, Object> store;
    private Cancellable cancellablePunctuator;
    private boolean isRunning;
    private final Random random = new Random();

    @Override
    public void init(final FixedKeyProcessorContext<K, V> context) {
        log.debug("initializing {} with context {}.{}", getClass().getSimpleName(), context.applicationId(), context.taskId());
        this.context = context;
        store = this.context.getStateStore(storeName);
        log.debug("{} retrieved store {}", getClass().getSimpleName(), store);

        cancellablePunctuator = context.schedule(
                punctuateInterval,
                punctuationType,
                punctuator
        );
    }

    @Override
    public void process(FixedKeyRecord<K, V> fixedKeyRecord) {
        context.forward(fixedKeyRecord);
    }

    @Override
    public void close() {
    }

    public Cancellable getCancellablePunctuator() {
        return cancellablePunctuator;
    }

    private class Updater extends Thread {

        @Override
        public void run() {
            isRunning = true;

            var actualStartTime = System.currentTimeMillis();
            log.trace("checking state store {} for records to remove older than {} at {} or actualStartTime {}", storeName, maxAge, Date.from(Instant.now()), actualStartTime);

            try (KeyValueIterator<Object, ValueAndTimestamp<Object>> storeRecords = store.range(UUID.randomUUID().toString(), null)) {
                //Note: with UUIDv7 this could start at a certain timestamp, maybe even with a reverse range scan

                while (store.isOpen() && storeRecords.hasNext()) {

                    long elapsedPunctuationTime = System.currentTimeMillis() - actualStartTime;
                    log.trace("elapsed time: {}, maxTime: {}", elapsedPunctuationTime, maxPunctuateMs);
                    if (elapsedPunctuationTime > maxPunctuateMs) {
                        log.trace("elapsed {} ms for punctuation of store {}, which is more than the allowed {} ms. Pausing iteration.", elapsedPunctuationTime, storeName, maxPunctuateMs);
                        break;
                    }

                    final KeyValue<Object, ValueAndTimestamp<Object>> rec = storeRecords.next();

                    boolean shouldDelete = deleteIfTrue.test(rec.value.timestamp(), actualStartTime);
                    log.trace("Checking record: {} shouldDelete {}", rec, shouldDelete);
                    if (shouldDelete && rec.value.value() != null) {
                        log.trace("removing value for key {} from store {}", rec.key, storeName);
                        store.delete(rec.key);
                    }
                    // TODO remove, just for testing maxPunctuationMs
                    try {
                        log.trace("Going to sleep");
                        sleep(Duration.ofSeconds(random.nextInt(0, 3)).toMillis());

                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

            } catch (InvalidStateStoreException ex) {
                log.trace("Store {} or Iterator has been closed", storeName);
            } finally {
                isRunning = false;
            }

            long elapsedPunctuationTime = System.currentTimeMillis() - actualStartTime;
            log.trace("time elapsed {} ms for punctuation of store {}", elapsedPunctuationTime, storeName);

        }
    }


    private final Punctuator punctuator = punctuateStartTime -> {

        if (!isRunning) {
            log.trace("Starting Thread");
            Updater updater = new Updater();
            updater.start();
            log.trace("Started Thread");
        } else {
            log.trace("Already running");
        }
    };

}
