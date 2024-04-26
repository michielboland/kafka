package org.boland.myapps;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.jetbrains.annotations.NotNull;

import java.util.Base64;
import java.util.Properties;
import java.util.Random;

public class Writer {

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
            String value = record.value();
            if (value == null) {
                return;
            }
            String[] a = value.split(" ");
            if (a.length != 2) {
                return;
            }
            long keyCount;
            long valueCount;
            try {
                keyCount = Long.parseLong(a[0]);
                valueCount = Long.parseLong(a[1]);
            } catch (NumberFormatException e) {
                return;
            }
            if (keyCount <= 0 || valueCount <= 0) {
                return;
            }
            random.longs(valueCount, 0, keyCount).forEach(k -> context.forward(new Record<>(String.valueOf(k), randomString(), record.timestamp())));
        }

        private String randomString() {
            byte[] bytes = new byte[15];
            random.nextBytes(bytes);
            return encoder.encodeToString(bytes);
        }
    }
}
