package org.boland.myapps;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

class Runner {

    void runStream(StreamsBuilder builder, Properties props) {
        final Topology topology = builder.build();
        try (KafkaStreams streams = new KafkaStreams(topology, props)) {
            runStreams(streams);
        }
    }

    private void runStreams(KafkaStreams streams) {
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
    }
}
