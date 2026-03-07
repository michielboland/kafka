package org.boland.kafka;

import org.apache.kafka.streams.TopologyConfig;
import org.apache.kafka.streams.processor.internals.InternalTopicProperties;

public class RepartitionProcessor {

    private static final String SOURCE = "source";
    private static final String REPARTITION_TOPIC = "output-repartition";

    public static void main(String[] args) {
        var config = Config.builder()
                .applicationId("streams-repartition-processor")
                .defaultBootstrapServer()
                .buildStreamsConfig();
        var topology = new ExtendedTopology(new TopologyConfig(config))
                .addInternalTopic(REPARTITION_TOPIC, new InternalTopicProperties(1))
                .addSource(SOURCE, "streams-plaintext-input")
                .addSink("output-repartition-sink", REPARTITION_TOPIC, SOURCE)
                .addSource("output-repartition-source", REPARTITION_TOPIC);
        new Runner().runStream(topology, config);
    }
}
