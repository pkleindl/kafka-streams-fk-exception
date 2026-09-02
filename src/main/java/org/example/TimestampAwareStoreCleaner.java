package org.example;

import org.apache.kafka.streams.KeyValue;
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
import java.util.function.BiPredicate;

public class TimestampAwareStoreCleaner<K, V> implements FixedKeyProcessor<K, V, V> {

    private static final Logger log = LoggerFactory.getLogger(TimestampAwareStoreCleaner.class);

    public TimestampAwareStoreCleaner(Duration punctuateInterval,
                                      Duration maxAge,
                                      String storeName) {
        this(punctuateInterval, maxAge, storeName, PunctuationType.WALL_CLOCK_TIME, Long.MAX_VALUE);
    }

    public TimestampAwareStoreCleaner(Duration punctuateInterval,
                                      Duration maxAge,
                                      String storeName,
                                      Long maxPunctuateMs) {
        this(punctuateInterval, maxAge, storeName, PunctuationType.WALL_CLOCK_TIME, maxPunctuateMs);
    }

    public TimestampAwareStoreCleaner(Duration punctuateInterval,
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
    private Duration maxAge;
    private BiPredicate<Long, Long> deleteIfTrue;
    private String storeName;
    private final PunctuationType punctuationType;

    private Long maxPunctuateMs;

    private FixedKeyProcessorContext<K, V> context;
    private TimestampedKeyValueStore<Object, Object> store;
    private Cancellable cancellablePunctuator;
    private KeyValueIterator<Object, ValueAndTimestamp<Object>> storeRecords;

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
        // nothing to do
    }

    public Cancellable getCancellablePunctuator() {
        return cancellablePunctuator;
    }

    private final Punctuator punctuator = punctuateStartTime -> {
        var actualStartTime = System.currentTimeMillis();
        log.trace("checking state store {} for records to remove older than {} at {} or actualStarTime {}", storeName, maxAge, punctuateStartTime, actualStartTime);

        if (storeRecords == null || !storeRecords.hasNext()) {
            storeRecords = store.all();
            log.trace("Starting a new iteration of the store {}", storeName);
        }

        try {
            log.trace("Going to sleep");
            Thread.sleep(Duration.ofSeconds(1).toMillis());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        while (storeRecords.hasNext()) {
            final KeyValue<Object, ValueAndTimestamp<Object>> rec = storeRecords.next();
            long elapsedPunctuationTime = System.currentTimeMillis() - actualStartTime;

            //log.trace("elapsed time: {}, maxTime: {}", elapsedPunctuationTime, maxPunctuateMs);
            if (elapsedPunctuationTime > this.maxPunctuateMs) {
                log.trace("elapsed {} ms for punctuation of store {}, which is more than the allowed {} ms. Pausing iteration.", elapsedPunctuationTime, storeName, maxPunctuateMs);
                break;
            }

            boolean shouldDelete = deleteIfTrue.test(rec.value.timestamp(), actualStartTime);
            if (shouldDelete && rec.value.value() != null) {
                log.debug("removing value for key {} from store {}", rec.key, storeName);
                store.delete(rec.key);
            }
        }
        long elapsedPunctuationTime = System.currentTimeMillis() - actualStartTime;
        log.trace("time elapsed {} ms for punctuation of store {}", elapsedPunctuationTime, storeName);
    };

}
