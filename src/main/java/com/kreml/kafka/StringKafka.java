package com.kreml.kafka;

import javafx.collections.ObservableList;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class StringKafka extends AbstractKafkaConsumer<String> {

    public StringKafka(ObservableList<String> observableList) {
        super(observableList);
    }

    @Override
    public KafkaConsumer<String, String> createConsumer() {
        Properties consumerProperties = getBaseConsumerProperties();
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Create the consumer using props.
        KafkaConsumer<String, String> myConsumer = new KafkaConsumer<>(consumerProperties);
        // Subscribe to the topic.
        myConsumer.subscribe(Collections.singletonList(getTopicName()), new PartitionOffsetAssignerListener(myConsumer, shouldSeekToEnd()));
        return myConsumer;
    }

    @Override
    String getRecordString(String value) {
        return value;
    }

}
