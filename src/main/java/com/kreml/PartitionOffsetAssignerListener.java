package com.kreml;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

public class PartitionOffsetAssignerListener implements ConsumerRebalanceListener {

    private KafkaConsumer consumer;
    private boolean shouldSeekToEnd;

    public PartitionOffsetAssignerListener(KafkaConsumer kafkaConsumer, boolean shouldSeekToEnd) {
        this.consumer = kafkaConsumer;
        this.shouldSeekToEnd = shouldSeekToEnd;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        if (shouldSeekToEnd) {
            consumer.seekToEnd(partitions);
        } else {
            consumer.seekToBeginning(partitions);
        }

    }
}
