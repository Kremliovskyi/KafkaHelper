package com.kreml.kafka;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class PartitionOffsetAssignerListener implements ConsumerRebalanceListener {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private KafkaConsumer consumer;
    private boolean shouldSeekToEnd;
    private Map<TopicPartition, Long> offsetStorage = new HashMap<>();

    PartitionOffsetAssignerListener(KafkaConsumer kafkaConsumer, boolean shouldSeekToEnd) {
        this.consumer = kafkaConsumer;
        this.shouldSeekToEnd = shouldSeekToEnd;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        if (!partitions.isEmpty()) {
            logger.warn("Partitions are being reassigned. Trying to save offsets.");
            partitions.forEach(topicPartition -> {
                offsetStorage.put(topicPartition, consumer.position(topicPartition));
            });
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        if (!offsetStorage.isEmpty()) {
            partitions.forEach(topicPartition -> {
                consumer.seek(topicPartition, offsetStorage.get(topicPartition));
                offsetStorage.remove(topicPartition);
            });
        } else {
            if (shouldSeekToEnd) {
                consumer.seekToEnd(partitions);
            } else {
                consumer.seekToBeginning(partitions);
            }
        }
    }
}
