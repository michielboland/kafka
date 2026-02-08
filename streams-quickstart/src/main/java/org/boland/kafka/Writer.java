package org.boland.kafka;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
        public void init(ProcessorContext<String, String> context) {
            this.context = context;
        }

        @Override
        public void process(Record<String, String> record) {
            long valueCount = Long.parseLong(record.value());
            if (valueCount <= 0) {
                return;
            }
            LOG.info("sending {} random strings", valueCount);
            LongStream.range(0, valueCount).forEach(k -> context.forward(new Record<>(randomWord(), randomString(), record.timestamp())));
        }

        private String randomWord() {
            byte[] bytes = new byte[3];
            random.nextBytes(bytes);
            return encoder.encodeToString(bytes);
        }

        private String randomString() {
            return IntStream.range(0, random.nextInt(16)).mapToObj(i -> randomWord()).collect(Collectors.joining(" "));
        }
    }
}
