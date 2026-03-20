package org.boland.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
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
        var properties = Config.builder()
                .defaultBootstrapServer()
                .applicationId("manual-consumer")
                .buildConsumerProperties();
        var timeout = Duration.ofSeconds(1);
        long lastCommit = System.nanoTime();
        try (var consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(Set.of("streams-plaintext-input"));
            while (!done.get()) {
                var consumerRecords = consumer.poll(timeout);
                if (done.get()) {
                    break;
                }
                LOGGER.atInfo().setMessage("poll returned {} records").addArgument(consumerRecords::count).log();
                long nanoTime = System.nanoTime();
                if (lastCommit + 10_000_000_000L <= nanoTime) {
                    LOGGER.atInfo().log("commit");
                    consumer.commitAsync();
                    lastCommit = nanoTime;
                }
            }
        } finally {
            latch.countDown();
        }
    }
}
