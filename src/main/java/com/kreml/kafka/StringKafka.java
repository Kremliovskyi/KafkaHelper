package com.kreml.kafka;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.kreml.DataProxy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.CharArrayReader;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class StringKafka {

    private static final String DE_SERIALIZER_NAME = StringDeserializer.class.getName();
    private static final String CONSUMER_GROUP_ID = "1";
    private KafkaConsumer<String, String> consumer;
    private String brokerAddress;
    private String topicName;
    private ExecutorService executor;
    private boolean shouldSeekToEnd;
    private DataProxy dataProxy;

    public StringKafka(DataProxy dataProxy) {
        this.dataProxy = dataProxy;
    }

    public StringKafka setBrokerAddress(String brokerAddress) {
        this.brokerAddress = brokerAddress;
        return this;
    }

    public StringKafka setTopicName(String topicName) {
        this.topicName = topicName;
        return this;
    }

    public StringKafka setShouldSeekToEnd(boolean shouldSeekToEnd) {
        this.shouldSeekToEnd = shouldSeekToEnd;
        return this;
    }

    public void runConsumer() {
        executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            consumer = createConsumer();
            AtomicInteger count = new AtomicInteger(1);
            while (!executor.isShutdown()) {
                final ConsumerRecords<String, String> consumerRecords =
                        consumer.poll(Duration.of(5L, ChronoUnit.SECONDS));

                StringBuilder result = new StringBuilder();
                consumerRecords.forEach(record -> {
                    result.append("================================ Count: ").append(count.getAndIncrement())
                            .append(" ================================================").append("\n");
                    Map<String, String> headersMap = new HashMap<>();
                    record.headers().forEach(h -> headersMap.put(h.key(), new String(h.value())));
                    if (!headersMap.isEmpty()) {
                        result.append("Headers: ");
                        headersMap.forEach((s, s2) -> result.append(s).append(":").append(s2).append(","));
                        result.append("\n");
                    }
                    String key = record.key();
                    if (key != null && !key.isEmpty()) {
                        result.append("Key: ").append(key).append("\n");
                    }
                    String recordString = record.value();
                    if (recordString != null && !recordString.isEmpty()) {
                        result.append("Value: ").append(logJson(recordString)).append("\n");
                        if (!executor.isShutdown()) {
                            System.out.println(result.toString());
                            dataProxy.data(result.toString());
                            result.setLength(0);
                        }
                    }
                });
                consumer.commitAsync();
            }
            consumer.close();
        });
    }

    public void stopConsumer() {
        executor.shutdownNow();
    }


    private KafkaConsumer<String, String> createConsumer() {
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, DE_SERIALIZER_NAME);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DE_SERIALIZER_NAME);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, shouldSeekToEnd ? OffsetResetStrategy.LATEST.name().toLowerCase() :
                OffsetResetStrategy.EARLIEST.name().toLowerCase());
        // Create the consumer using props.
        KafkaConsumer<String, String> myConsumer = new KafkaConsumer<>(consumerProperties);
        // Subscribe to the topic.
        myConsumer.subscribe(Collections.singletonList(topicName), new PartitionOffsetAssignerListener(myConsumer, shouldSeekToEnd));
        return myConsumer;
    }

    private void seekToEnd(KafkaConsumer<String, String> consumer) {
        ConsumerRecords records;
        Duration timeoutInSeconds = Duration.ofSeconds(5);
        consumer.poll(timeoutInSeconds);
        consumer.commitSync();
        consumer.seekToEnd(new ArrayList<>());
        do {
            records = consumer.poll(timeoutInSeconds);
            consumer.commitSync();
        } while(!records.isEmpty());
    }

    private String logJson(String JSONString) {
        Gson gson = new GsonBuilder().setPrettyPrinting().setLenient().create();
        JsonReader r = new JsonReader(new CharArrayReader(JSONString.toCharArray()));
        r.setLenient(true);
        JsonParser jp = new JsonParser();
        JsonElement je = jp.parse(r);
        return gson.toJson(je);
    }
}
