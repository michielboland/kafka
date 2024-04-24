package org.boland.myapps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

public class Writer {

    public static void main(String[] args) {
        Properties props = Config.builder()
                .applicationId("dummy-writer")
                .defaultBootstrapServer()
                .build();
        var builder = new StreamsBuilder();

        builder.addStateStore(Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("dummyStore"), new Serdes.StringSerde(), new Serdes.StringSerde()));
        builder.<String, String>stream("dummy")
                .process(DummyProcessor::new, "dummyStore")
                .to("streams-plaintext-input");
        new Runner().runStream(builder, props);
    }

    private static class DummyProcessor implements Processor<String, String, String, String> {

        private KeyValueStore<String, String> dummyStore;

        @Override
        public void init(@NotNull ProcessorContext<String, String> context) {
            dummyStore = context.getStateStore("dummyStore");
            context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, new DummyPunctuator(context, dummyStore));
        }

        @Override
        public void process(@NotNull Record<String, String> record) {
            final String key;
            if (record.key() == null && record.value() != null) {
                key = record.value().substring(0, 4);
            } else {
                key = record.key();
            }
            dummyStore.put(String.valueOf(key), record.value());
        }
    }

    private static class DummyPunctuator implements Punctuator {

        private static final Logger LOG = LoggerFactory.getLogger(DummyPunctuator.class);
        private final ProcessorContext<String, String> context;
        private final KeyValueStore<String, String> dummyStore;

        private DummyPunctuator(ProcessorContext<String, String> context, KeyValueStore<String, String> dummyStore) {
            this.context = context;
            this.dummyStore = dummyStore;
        }

        @Override
        public void punctuate(long timestamp) {
            KeyValue<String, String> keyValue;
            try (KeyValueIterator<String, String> iter = dummyStore.all()) {
                if (iter.hasNext()) {
                    keyValue = iter.next();
                } else {
                    return;
                }
            }
            LOG.atInfo().setMessage("outgoing").addKeyValue("key", keyValue.key).addKeyValue("value", keyValue.value).log();
            context.forward(new Record<>(keyValue.key, keyValue.value, timestamp));
            dummyStore.delete(keyValue.key);
        }
    }
}
