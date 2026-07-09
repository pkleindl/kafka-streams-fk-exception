This reproduces an exception we encountered in Kafka Streams.

The topology has a foreign-key left join where the left-side state store has a Punctuator attached to perform regular cleanups of old records.

After the punctuator deletes a record from the log, the stream crashes with "java.lang.IllegalArgumentException: Invalid partition: -1. Partition number should always be non-negative or null."

The exception only happens if StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG is not 0.

The resulting exception is:

```
19:26:27.115 [fk-join-invalid-partition-it-31764876-b0bf-43a9-b0b1-7a75f89f3645-42a68a35-5e07-4b09-8353-a1828b9f5d3c-StreamThread-1] ERROR o.apache.kafka.streams.KafkaStreams - stream-client [fk-join-invalid-partition-it-31764876-b0bf-43a9-b0b1-7a75f89f3645-42a68a35-5e07-4b09-8353-a1828b9f5d3c] Encountered the following exception during processing and sent shutdown request for the entire application.
org.apache.kafka.streams.errors.StreamsException: Exception caught in process. taskId=1_5, processor=join-subscription-join-foreign, topic=fk-join-invalid-partition-it-31764876-b0bf-43a9-b0b1-7a75f89f3645-join-subscription-registration-topic, partition=5, offset=0
	at org.apache.kafka.streams.processor.internals.StreamTask.handleException(StreamTask.java:858)
	at org.apache.kafka.streams.processor.internals.StreamTask.handleException(StreamTask.java:840)
	at org.apache.kafka.streams.processor.internals.StreamTask.handleException(StreamTask.java:855)
	at org.apache.kafka.streams.processor.internals.StreamTask.process(StreamTask.java:822)
	at org.apache.kafka.streams.processor.internals.TaskExecutor.processTask(TaskExecutor.java:95)
	at org.apache.kafka.streams.processor.internals.TaskExecutor.process(TaskExecutor.java:76)
	at org.apache.kafka.streams.processor.internals.TaskManager.process(TaskManager.java:1934)
	at org.apache.kafka.streams.processor.internals.StreamThread.runOnceWithoutProcessingThreads(StreamThread.java:1238)
	at org.apache.kafka.streams.processor.internals.StreamThread.runLoop(StreamThread.java:956)
	at org.apache.kafka.streams.processor.internals.StreamThread.run(StreamThread.java:916)
Caused by: java.lang.IllegalArgumentException: Invalid partition: -1. Partition number should always be non-negative or null.
	at org.apache.kafka.clients.producer.ProducerRecord.<init>(ProducerRecord.java:77)
	at org.apache.kafka.streams.processor.internals.RecordCollectorImpl.send(RecordCollectorImpl.java:259)
	at org.apache.kafka.streams.processor.internals.RecordCollectorImpl.send(RecordCollectorImpl.java:175)
	at org.apache.kafka.streams.processor.internals.SinkNode.process(SinkNode.java:96)
	at org.apache.kafka.streams.processor.internals.ProcessorContextImpl.forwardInternal(ProcessorContextImpl.java:293)
	at org.apache.kafka.streams.processor.internals.ProcessorContextImpl.forward(ProcessorContextImpl.java:272)
	at org.apache.kafka.streams.processor.internals.ProcessorContextImpl.forward(ProcessorContextImpl.java:228)
	at org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionJoinProcessorSupplier$1.process(SubscriptionJoinProcessorSupplier.java:98)
	at org.apache.kafka.streams.processor.internals.ProcessorNode.process(ProcessorNode.java:179)
	at org.apache.kafka.streams.processor.internals.ProcessorContextImpl.forwardInternal(ProcessorContextImpl.java:293)
	at org.apache.kafka.streams.processor.internals.ProcessorContextImpl.forward(ProcessorContextImpl.java:272)
	at org.apache.kafka.streams.processor.internals.ProcessorContextImpl.forward(ProcessorContextImpl.java:228)
	at org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionReceiveProcessorSupplier$1.process(SubscriptionReceiveProcessorSupplier.java:96)
	at org.apache.kafka.streams.processor.internals.ProcessorNode.process(ProcessorNode.java:179)
	at org.apache.kafka.streams.processor.internals.ProcessorContextImpl.forwardInternal(ProcessorContextImpl.java:293)
	at org.apache.kafka.streams.processor.internals.ProcessorContextImpl.forward(ProcessorContextImpl.java:272)
	at org.apache.kafka.streams.processor.internals.ProcessorContextImpl.forward(ProcessorContextImpl.java:228)
	at org.apache.kafka.streams.processor.internals.SourceNode.process(SourceNode.java:95)
	at org.apache.kafka.streams.processor.internals.StreamTask.lambda$doProcess$0(StreamTask.java:888)
	at org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.maybeMeasureLatency(StreamsMetricsImpl.java:955)
	at org.apache.kafka.streams.processor.internals.StreamTask.doProcess(StreamTask.java:888)
	at org.apache.kafka.streams.processor.internals.StreamTask.process(StreamTask.java:792)
	... 6 common frames omitted
```
