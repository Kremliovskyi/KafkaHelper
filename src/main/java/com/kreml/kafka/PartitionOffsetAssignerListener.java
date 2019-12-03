package com.kreml.kafka;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;

public class PartitionOffsetAssignerListener implements ConsumerRebalanceListener {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private KafkaConsumer consumer;
    private boolean shouldSeekToEnd;
    private static Collection<TopicPartition> assignedPartitions = new ArrayList<>();

    PartitionOffsetAssignerListener(KafkaConsumer kafkaConsumer, boolean shouldSeekToEnd) {
        this.consumer = kafkaConsumer;
        this.shouldSeekToEnd = shouldSeekToEnd;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        if (assignedPartitions.isEmpty()) {
            assignedPartitions.addAll(partitions);
        } else {
            if (partitions.removeAll(assignedPartitions)) {
                logger.warn("Duplicate partitions were reassigned.");
            }
            assignedPartitions.addAll(partitions);
        }
        if (shouldSeekToEnd) {
            consumer.seekToEnd(partitions);
        } else {
            consumer.seekToBeginning(partitions);
        }
    }
}
