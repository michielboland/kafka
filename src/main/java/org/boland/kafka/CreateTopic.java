package org.boland.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.List;
import java.util.Properties;

public class CreateTopic {

    public static void main(String[] args) {
        Properties props = Config.builder().defaultBootstrapServer().applicationId("create-topic").build();
        try (AdminClient adminClient = AdminClient.create(props)) {
            adminClient.createTopics(List.of(new NewTopic("dummy", 2, (short) 1)));
        }
    }
}
