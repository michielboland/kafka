package org.boland.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class ManualConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ManualConsumer.class);

    private final AtomicBoolean done = new AtomicBoolean();
    private final CountDownLatch latch = new CountDownLatch(1);

    public static void main(String[] args) {
        var manualConsumer = new ManualConsumer();
        Runtime.getRuntime().addShutdownHook(new Thread(manualConsumer::done, "consumer-shutdown"));
        manualConsumer.run();
    }

    void done() {
        if (!done.getAndSet(true)) {
            try {
                LOGGER.atInfo().log("waiting for consumer to stop");
                latch.await();
                LOGGER.atInfo().log("done");
            } catch (InterruptedException e) {
                LOGGER.atError().setCause(e).log("interrupted");
            }
        }
    }

    void run() {
        var timeout = Duration.ofSeconds(1);
        long lastCommit = System.nanoTime();
        try (var consumer = consumer(); var producer = producer()) {
            producer.initTransactions();
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            consumer.subscribe(Set.of("streams-plaintext-input"));
            while (!done.get()) {
                var consumerRecords = consumer.poll(timeout);
                if (done.get()) {
                    break;
                }
                consumerRecords.forEach(consumerRecord -> updateOffsets(consumerRecord, offsets));
                LOGGER.atInfo().setMessage("poll returned {} records").addArgument(consumerRecords::count).log();
                long nanoTime = System.nanoTime();
                if (lastCommit + 10_000_000_000L <= nanoTime) {
                    commit(consumer, producer, offsets);
                    lastCommit = nanoTime;
                }
            }
        } finally {
            latch.countDown();
        }
    }

    private void updateOffsets(ConsumerRecord<String, String> consumerRecord, Map<TopicPartition, OffsetAndMetadata> offsets) {
        offsets.put(
                new TopicPartition(
                        consumerRecord.topic(),
                        consumerRecord.partition()
                ),
                new OffsetAndMetadata(
                        consumerRecord.offset() + 1,
                        consumerRecord.leaderEpoch(),
                        null
                ));
    }

    void commit(KafkaConsumer<String, String> consumer, KafkaProducer<String, String> producer, Map<TopicPartition, OffsetAndMetadata> offsetsToCommit) {
        if (offsetsToCommit.isEmpty()) {
            LOGGER.atInfo().log("nothing to commit");
            return;
        }
        LOGGER.atInfo().log("commit");
        producer.beginTransaction();
        // TODO send something here
        producer.sendOffsetsToTransaction(offsetsToCommit, consumer.groupMetadata());
        producer.commitTransaction();
        offsetsToCommit.clear();
    }

    private KafkaConsumer<String, String> consumer() {
        return new KafkaConsumer<>(Config.builder()
                .defaultBootstrapServer()
                .applicationId("manual-consumer")
                .buildConsumerProperties(), new StringDeserializer(), new StringDeserializer());
    }

    private KafkaProducer<String, String> producer() {
        return new KafkaProducer<>(Config.builder()
                .defaultBootstrapServer()
                .applicationId("manual-consumer")
                .buildProducerProperties(), new StringSerializer(), new StringSerializer());
    }
}
