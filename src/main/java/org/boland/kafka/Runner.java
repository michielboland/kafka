package org.boland.kafka;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;

class Runner {

    private static final Logger LOG = LoggerFactory.getLogger(Runner.class);
    private boolean close = true;

    void runStream(StreamsBuilder builder, Properties props) {
        final Topology topology = builder.build();
        int status;
        try (KafkaStreams streams = new KafkaStreams(topology, props)) {
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
            runnerThread.interrupt();
            return SHUTDOWN_CLIENT;
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
