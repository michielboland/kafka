package org.boland.myapps;

import org.apache.kafka.streams.StreamsBuilder;

import java.util.Arrays;
import java.util.Properties;

/**
 * In this example, we implement a simple LineSplit program using the high-level Streams DSL
 * that reads from a source topic "streams-plaintext-input", where the values of messages represent lines of text;
 * the code split each text line in string into words and then write back into a sink topic "streams-linesplit-output" where
 * each record represents a single word.
 */
public class LineSplit {

    public static void main(String[] args) {
        Properties props = Config.builder()
                .applicationId("streams-linesplit")
                .defaultBootstrapServer()
                .build();

        final StreamsBuilder builder = new StreamsBuilder();

        builder.<String, String>stream("streams-plaintext-input")
            .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
            .to("streams-linesplit-output");

        new Runner().runStream(builder, props);
    }
}
