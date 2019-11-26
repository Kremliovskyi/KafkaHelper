package com.kreml.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;

public class PartitionOffsetAssignerListener implements ConsumerRebalanceListener {

    private final Logger logger = LogManager.getLogger();

    private Consumer consumer;
    private boolean shouldSeekToEnd;
    private Collection<TopicPartition> revokedPartitions = new ArrayList<>();

    public PartitionOffsetAssignerListener(Consumer kafkaConsumer, boolean shouldSeekToEnd) {
        this.consumer = kafkaConsumer;
        this.shouldSeekToEnd = shouldSeekToEnd;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        logger.debug("Partitions are revoked: " + partitions);
        revokedPartitions.addAll(partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        if (revokedPartitions.isEmpty()) {
            if (shouldSeekToEnd) {
                consumer.seekToEnd(partitions);
            } else {
                consumer.seekToBeginning(partitions);
            }
        } else {
            revokedPartitions.clear();
        }
    }
}
