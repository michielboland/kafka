package org.boland.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;

public class States {

    public static void main(String[] args) {
        var config = Config.builder()
                .applicationId("states")
                .defaultBootstrapServer()
                .buildStreamsConfig();
        final StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream("streams-plaintext-input")
                .process(new StateProcessorSupplier());
        new Runner().runStream(builder, config);
    }

    private static class StateProcessorSupplier implements ProcessorSupplier<String, String, String, String> {

        private static final String TEST_STORE = "test-store";
        private final StoreBuilder<KeyValueStore<String, Long>> storeBuilder = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(TEST_STORE), new Serdes.StringSerde(), new Serdes.LongSerde());

        @Override
        public Processor<String, String, String, String> get() {
            return new StateProcessor();
        }

        @Override
        public Set<StoreBuilder<?>> stores() {
            return Collections.singleton(storeBuilder);
        }

        private static class StateProcessor implements Processor<String, String, String, String> {

            private static final Logger LOGGER = LoggerFactory.getLogger(StateProcessor.class);
            private KeyValueStore<String, Long> store;

            @Override
            public void init(ProcessorContext<String, String> context) {
                store = context.getStateStore(TEST_STORE);
            }

            @Override
            public void process(Record<String, String> record) {
                String key = record.key();
                if (key == null) {
                    return;
                }
                Long l = store.get(key);
                long n = l == null ? 1L : l + 1;
                if ((n % 8L) == 0) {
                    LOGGER.atInfo().setMessage("deleting {}").addArgument(key).log();
                    store.delete(key);
                } else {
                    store.put(key, n);
                }
            }
        }
    }
}
