package com.kreml.kafka;

import com.kreml.RecordsProxy;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.net.URI;
import java.util.Collections;
import java.util.Properties;

public class AvroKafka extends AbstractKafkaConsumer<GenericData.Record> {

    private String schemaRegistry;

    public AvroKafka(RecordsProxy recordsProxy, String schemaRegistry) {
        super(recordsProxy);
        this.schemaRegistry = schemaRegistry;
    }

    @Override
    public KafkaConsumer<String, GenericData.Record> createConsumer() {
        Properties consumerProperties = getBaseConsumerProperties();
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        consumerProperties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, URI.create(schemaRegistry));
        // Create the consumer using props.
        KafkaConsumer<String, GenericData.Record> myConsumer = new KafkaConsumer<>(consumerProperties);
        // Subscribe to the topic.
        myConsumer.subscribe(Collections.singletonList(getTopicName()), new PartitionOffsetAssignerListener(myConsumer, shouldSeekToEnd()));
        return myConsumer;
    }

    @Override
    String getRecordString(GenericData.Record value) {
        return value.toString();
    }
}
