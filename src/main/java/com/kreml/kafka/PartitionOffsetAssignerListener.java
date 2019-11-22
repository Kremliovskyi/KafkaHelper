package com.kreml.kafka;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collection;

public class PartitionOffsetAssignerListener implements ConsumerRebalanceListener {

    private KafkaConsumer consumer;
    private boolean shouldSeekToEnd;
    private Collection<TopicPartition> assignedPartitions = new ArrayList<>();

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
                System.out.println("partitions.removeAll(assignedPartitions) is success");
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
