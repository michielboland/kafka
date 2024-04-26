package org.boland.kafka;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import java.util.Properties;

class Config {

    @NotNull
    @Contract(value = " -> new", pure = true)
    static Builder builder() {
        return new Builder();
    }

    static class Builder {
        private String applicationId;
        private String bootstrapServer;

        Builder applicationId(String applicationId) {
            this.applicationId = applicationId;
            return this;
        }

        Builder defaultBootstrapServer() {
            this.bootstrapServer = "localhost:9092";
            return this;
        }

        Properties build() {
            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, Objects.requireNonNull(applicationId));
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Objects.requireNonNull(bootstrapServer));
            props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
            Class<?> stringSerdeClass;
            try (Serde<String> stringSerde = Serdes.String()) {
                stringSerdeClass = stringSerde.getClass();
            }
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerdeClass);
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, stringSerdeClass);
            return props;
        }
    }
}
