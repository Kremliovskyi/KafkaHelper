package com.kreml;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.CharArrayReader;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class StringKafka {

    private static final String DE_SERIALIZER_NAME = StringDeserializer.class.getName();
    private static final String CONSUMER_GROUP_ID = "1";
    private KafkaConsumer<String, String> consumer;
    private String brokerAddress;
    private String topicName;
    private boolean shouldContinue;
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
//            if (shouldSeekToEnd) {
//                seekToEnd(consumer);
//            } else {
//                seekToBeginning(consumer);
//            }
            AtomicInteger count = new AtomicInteger(1);
            while (shouldContinue) {
                final ConsumerRecords<String, String> consumerRecords =
                        consumer.poll(Duration.of(5L, ChronoUnit.SECONDS));

                StringBuilder result = new StringBuilder();
                consumerRecords.forEach(record -> {
                    Map<String, String> headersMap = new HashMap<>();
                    record.headers().forEach(h -> headersMap.put(h.key(), new String(h.value())));
                    if (!headersMap.isEmpty()) {
                        result.append("Headers: ");
                        headersMap.forEach((s, s2) -> result.append(s).append(":").append(s2).append(","));
                        result.append("\n");
                    }
                    result.append("Key: ").append(record.key()).append("\n");
                    result.append("Value: ").append(logJson(record.value())).append("\n");
                    result.append("================================ Count: ").append(count.getAndIncrement())
                            .append(" ================================================").append("\n");
                });
                if (result.length() > 0 && shouldContinue) {
                    dataProxy.data(result.toString());
                }
                consumer.commitAsync();
            }
            consumer.close();
        });
    }

    void stopConsumer() {
        shouldContinue = true;
        executor.shutdown();
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

    private void seekToBeginning(KafkaConsumer<String, String> consumer) {
        Duration timeoutInSeconds = Duration.ofSeconds(5);
        consumer.poll(timeoutInSeconds);
        consumer.seekToBeginning(new ArrayList<>());
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
