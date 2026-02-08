package org.boland.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class CreateTopics {

    private static final String[] TOPICS = {
            "dummy",
            "streams-plaintext-input",
            "streams-linesplit-output",
            "streams-pipe-output",
            "streams-wordcount-output"
    };

    public static void main(String[] args) throws Exception {
        Properties props = Config.builder().defaultBootstrapServer().applicationId("create-topic").build();
        try (var adminClient = Admin.create(props)) {
            adminClient.createTopics(Arrays.stream(TOPICS).map(n -> new NewTopic(n, 2, (short) 1)).collect(Collectors.toList())).all().get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new RuntimeException(e);
            }
        }
    }
}
