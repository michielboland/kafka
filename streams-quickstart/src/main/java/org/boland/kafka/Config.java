package org.boland.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Objects;
import java.util.Properties;

class Config {

    static final int REPLICATION_FACTOR = 1;

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

        Properties buildClientProperties() {
            var props = new Properties();
            props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            return props;
        }

        Properties buildStreamsProperties() {
            var props = buildClientProperties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, Objects.requireNonNull(applicationId));
            props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
            props.put(StreamsConfig.STATE_DIR_CONFIG, "target/kafka-streams");
            props.put(StreamsConfig.ENSURE_EXPLICIT_INTERNAL_RESOURCE_NAMING_CONFIG, true);
            props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, REPLICATION_FACTOR);
            return props;
        }

        Properties buildConsumerProperties() {
            var props = buildClientProperties();
            props.put(ConsumerConfig.GROUP_ID_CONFIG, Objects.requireNonNull(applicationId));
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
            props.put(ConsumerConfig.DEFAULT_ISOLATION_LEVEL, "read_committed");
            return props;
        }

        Properties buildProducerProperties() {
            var props = buildClientProperties();
            props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, Objects.requireNonNull(applicationId));
            return props;
        }

        StreamsConfig buildStreamsConfig() {
            return new StreamsConfig(buildStreamsProperties());
        }
    }
}
