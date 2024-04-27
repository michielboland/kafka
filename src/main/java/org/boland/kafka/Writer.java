package org.boland.kafka;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.Properties;
import java.util.Random;
import java.util.stream.LongStream;

public class Writer {

    private static final Logger LOG = LoggerFactory.getLogger(Writer.class);

    public static void main(String[] args) {
        Properties props = Config.builder()
                .applicationId("dummy-writer")
                .defaultBootstrapServer()
                .build();
        var builder = new StreamsBuilder();
        builder.<String, String>stream("dummy")
                .process(DummyProcessor::new)
                .to("streams-plaintext-input");
        new Runner().runStream(builder, props);
    }

    private static class DummyProcessor implements Processor<String, String, String, String> {

        private final Random random = new Random();
        private final Base64.Encoder encoder = Base64.getEncoder();
        private ProcessorContext<String, String> context;

        @Override
        public void init(@NotNull ProcessorContext<String, String> context) {
            this.context = context;
        }

        @Override
        public void process(@NotNull Record<String, String> record) {
            String key = record.key();
            if (key == null) {
                return;
            }
            String value = record.value();
            if (value == null) {
                return;
            }
            long valueCount;
            try {
                valueCount = Long.parseLong(value);
            } catch (NumberFormatException e) {
                return;
            }
            if (valueCount <= 0) {
                return;
            }
            LOG.info("sending {} random strings to '{}'", valueCount, key);
            LongStream.range(0, valueCount).forEach(k -> context.forward(new Record<>(key, randomString(), record.timestamp())));
        }

        private String randomString() {
            byte[] bytes = new byte[3];
            random.nextBytes(bytes);
            return encoder.encodeToString(bytes);
        }
    }
}
