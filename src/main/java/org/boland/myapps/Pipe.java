package org.boland.myapps;

import org.apache.kafka.streams.StreamsBuilder;

import java.util.Properties;

/**
 * In this example, we implement a simple Pipe program using the high-level Streams DSL
 * that reads from a source topic "streams-plaintext-input", where the values of messages represent lines of text,
 * and writes the messages as-is into a sink topic "streams-pipe-output".
 */
public class Pipe {

    public static void main(String[] args) {
        Properties props = Config.builder()
                .applicationId("streams-pipe")
                .defaultBootstrapServer()
                .build();

        final StreamsBuilder builder = new StreamsBuilder();

        builder.stream("streams-plaintext-input").to("streams-pipe-output");

        new Runner().runStream(builder, props);
    }
}
