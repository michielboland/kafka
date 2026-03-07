package org.boland.kafka;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyConfig;
import org.apache.kafka.streams.kstream.Repartitioned;

public class Repartition {

    public static void main(String[] args) {
        var config = Config.builder()
                .applicationId("streams-repartition")
                .defaultBootstrapServer()
                .buildStreamsConfig();
        var builder = new StreamsBuilder(new TopologyConfig(config));
        builder.stream("streams-plaintext-input")
                .repartition(Repartitioned.as("output").withNumberOfPartitions(1));
        new Runner().runStream(builder, config);
    }
}
