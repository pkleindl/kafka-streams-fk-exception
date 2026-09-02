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
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;

public class TimestampAwareStoreCleaner2<K, V> implements FixedKeyProcessor<K, V, V> {

    private static final Logger log = LoggerFactory.getLogger(TimestampAwareStoreCleaner2.class);

    public TimestampAwareStoreCleaner2(Duration punctuateInterval,
                                       Duration maxAge,
                                       String storeName) {
        this(punctuateInterval, maxAge, storeName, PunctuationType.WALL_CLOCK_TIME, Long.MAX_VALUE);
    }

    public TimestampAwareStoreCleaner2(Duration punctuateInterval,
                                       Duration maxAge,
                                       String storeName,
                                       Long maxPunctuateMs) {
        this(punctuateInterval, maxAge, storeName, PunctuationType.WALL_CLOCK_TIME, maxPunctuateMs);
    }

    public TimestampAwareStoreCleaner2(Duration punctuateInterval,
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
        storeRecords.close();
    }

    public Cancellable getCancellablePunctuator() {
        return cancellablePunctuator;
    }

    private class Updater extends Thread {
        //KeyValueIterator<Object, ValueAndTimestamp<Object>> storeRecords;

        @Override
        public void run() {
            store.flush();
            var actualStartTime = System.currentTimeMillis();
            log.trace("checking state store {} for records to remove older than {} at {} or actualStarTime {}", storeName, maxAge, Date.from(Instant.now()), actualStartTime);

            if (storeRecords == null || !storeRecords.hasNext()) {
                storeRecords = store.all();
                log.trace("Starting a new iteration of the store {}", storeName);
            }

            while (storeRecords.hasNext()) {
                final KeyValue<Object, ValueAndTimestamp<Object>> rec = storeRecords.next();
                long elapsedPunctuationTime = System.currentTimeMillis() - actualStartTime;

                //log.trace("elapsed time: {}, maxTime: {}", elapsedPunctuationTime, maxPunctuateMs);
                if (elapsedPunctuationTime > Duration.ofSeconds(30).toMillis()) {
                    log.trace("elapsed {} ms for punctuation of store {}, which is more than the allowed {} ms. Pausing iteration.", elapsedPunctuationTime, storeName, maxPunctuateMs);
                    break;
                }

                boolean shouldDelete = deleteIfTrue.test(rec.value.timestamp(), actualStartTime);
                if (shouldDelete && rec.value.value() != null) {
                    log.debug("removing value for key {} from store {}", rec.key, storeName);
                    store.delete(rec.key);
                }
            }
            try {
                log.trace("Going to sleep");
                sleep(Duration.ofSeconds(5).toMillis());

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            long elapsedPunctuationTime = System.currentTimeMillis() - actualStartTime;
            log.trace("time elapsed {} ms for punctuation of store {}", elapsedPunctuationTime, storeName);

        }
    }


    private final Punctuator punctuator = punctuateStartTime -> {
        log.trace("Starting Thread");
        Updater updater = new Updater();
        updater.start();
        log.trace("Started Thread");
    };

}
