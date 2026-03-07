package org.boland.kafka;

import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyConfig;
import org.apache.kafka.streams.processor.internals.InternalTopicProperties;

public class ExtendedTopology extends Topology {

    public ExtendedTopology(TopologyConfig topologyConfig) {
        super(topologyConfig);
    }

    public Topology addInternalTopic(String topicName, InternalTopicProperties internalTopicProperties) {
        internalTopologyBuilder.addInternalTopic(topicName, internalTopicProperties);
        return this;
    }
}
