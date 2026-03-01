package org.boland.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class CreateTopics {

    private static final String[] TOPICS = {
            "dummy",
            "streams-plaintext-input",
            "streams-linesplit-output",
            "streams-pipe-output",
            "streams-wordcount-output"
    };
    private static final int NUM_PARTITIONS = 2;
    private static final short REPLICATION_FACTOR = 1;

    public static void main(String[] args) throws Exception {
        Properties props = Config.builder()
                .defaultBootstrapServer()
                .applicationId("create-topic")
                .build();
        try (var adminClient = Admin.create(props)) {
            adminClient.createTopics(Arrays.stream(TOPICS)
                            .map(topicName -> new NewTopic(topicName, NUM_PARTITIONS, REPLICATION_FACTOR))
                            .toList())
                    .all()
                    .get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new RuntimeException(e);
            }
        }
    }
}
