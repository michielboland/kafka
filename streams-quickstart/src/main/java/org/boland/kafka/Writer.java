package org.boland.kafka;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyConfig;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class Writer {

    private static final Logger LOG = LoggerFactory.getLogger(Writer.class);

    public static void main(String[] args) {
        var config = Config.builder()
                .applicationId("writer")
                .defaultBootstrapServer()
                .buildStreamsConfig();
        var builder = new StreamsBuilder(new TopologyConfig(config));
        builder.<String, String>stream("writer-input")
                .process(WriteProcessor::new)
                .to("streams-plaintext-input");
        new Runner().runStream(builder, config);
    }

    private static class WriteProcessor implements Processor<String, String, String, String> {

        private final List<String> randomwords;
        private final Random random = new Random();
        private ProcessorContext<String, String> context;

        {
            try (var reader = new BufferedReader(new InputStreamReader(Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResourceAsStream("randomwords"))))) {
                randomwords = reader.lines().toList();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

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
            return randomwords.get(random.nextInt(randomwords.size()));
        }

        private String randomString() {
            return IntStream.range(0, random.nextInt(16) + 1).mapToObj(i -> randomWord()).collect(Collectors.joining(" "));
        }
    }
}
