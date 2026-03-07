package org.boland.kafka;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;

class Runner {

    private static final Logger LOG = LoggerFactory.getLogger(Runner.class);
    private boolean close = true;

    void runStream(StreamsBuilder builder, StreamsConfig config) {
        final Topology topology = builder.build();
        int status;
        try (KafkaStreams streams = new KafkaStreams(topology, config)) {
            status = runStreams(streams);
        }
        close = false;
        System.exit(status);
    }

    private int runStreams(KafkaStreams streams) {
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                if (close) {
                    LOG.warn("Caught SIGINT - shutting down");
                    streams.close();
                    latch.countDown();
                }
            }
        });

        Thread runnerThread = Thread.currentThread();
        streams.setUncaughtExceptionHandler(e -> {
            LOG.warn("Sending shutdown request following uncaught exception");
            runnerThread.interrupt();
            return SHUTDOWN_APPLICATION;
        });
        streams.start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            return 1;
        }
        return 0;
    }
}
