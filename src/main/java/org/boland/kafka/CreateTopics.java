package org.boland.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;

public class CreateTopics {

    private static final String[] TOPICS = {
            "dummy",
            "streams-plaintext-input",
            "streams-linesplit-output",
            "streams-pipe-output",
            "streams-wordcount-output"
    };

    public static void main(String[] args) {
        Properties props = Config.builder().defaultBootstrapServer().applicationId("create-topic").build();
        try (AdminClient adminClient = AdminClient.create(props)) {
            adminClient.createTopics(Arrays.stream(TOPICS).map(n -> new NewTopic(n, 2, (short) 1)).collect(Collectors.toList()));
        }
    }
}
