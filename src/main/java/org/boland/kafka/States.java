package org.boland.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Unmodifiable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

public class States {

    private static final Logger LOG = LoggerFactory.getLogger(States.class);

    public static void main(String[] args) {
        Properties props = Config.builder()
                .applicationId("states")
                .defaultBootstrapServer()
                .build();
        final StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream("streams-plaintext-input")
                .process(new StateProcessorSupplier(Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("test-store"), new Serdes.StringSerde(), new Serdes.LongSerde())));
        new Runner().runStream(builder, props);
    }

    private static class StateProcessorSupplier implements ProcessorSupplier<String, String, String, String> {

        private final StoreBuilder<KeyValueStore<String, Long>> storeBuilder;

        private StateProcessorSupplier(StoreBuilder<KeyValueStore<String, Long>> storeBuilder) {
            this.storeBuilder = storeBuilder;
        }

        @Contract(" -> new")
        @Override
        public @NotNull Processor<String, String, String, String> get() {
            return new StateProcessor();
        }

        @Contract(value = " -> new", pure = true)
        @Override
        public @NotNull @Unmodifiable Set<StoreBuilder<?>> stores() {
            return Collections.singleton(storeBuilder);
        }
    }

    private static class StateProcessor implements Processor<String, String, String, String>, Punctuator {
        private KeyValueStore<String, Long> store;

        @Override
        public void init(@NotNull ProcessorContext<String, String> context) {
            store = context.getStateStore("test-store");
            context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, this);
        }

        @Override
        public void process(@NotNull Record<String, String> record) {
            String key = record.key();
            if (key == null) {
                return;
            }
            Long l = store.get(key);
            long n = l == null ? 1L : l + 1;
            if ((n % 8L) == 0) {
                store.delete(key);
            } else {
                store.put(key, n);
            }
        }

        @Override
        public void punctuate(long timestamp) {
            try (KeyValueIterator<String, Long> iterator = store.all()) {
                while (iterator.hasNext()) {
                    KeyValue<String, Long> next = iterator.next();
                    LOG.info(" k={}, v={}", next.key, next.value);
                }
            }
        }
    }
}
