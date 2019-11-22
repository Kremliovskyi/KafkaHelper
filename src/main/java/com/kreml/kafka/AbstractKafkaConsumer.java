package com.kreml.kafka;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.kreml.RecordsProxy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.CharArrayReader;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractKafkaConsumer<V> {

    static final String CONSUMER_GROUP_ID = "1";
    private KafkaConsumer<String, V> consumer;
    private String brokerAddress;
    private String topicName;
    private ExecutorService executor;
    private boolean shouldSeekToEnd;
    private RecordsProxy recordsProxy;
    private AtomicInteger count = new AtomicInteger(1);

    abstract KafkaConsumer<String, V> createConsumer();

    abstract String getRecordString(V value);

    public AbstractKafkaConsumer(RecordsProxy recordsProxy) {
        this.recordsProxy = recordsProxy;
    }

    public String getBrokerAddress() {
        return brokerAddress;
    }

    public AbstractKafkaConsumer setBrokerAddress(String brokerAddress) {
        this.brokerAddress = brokerAddress;
        return this;
    }

    public AbstractKafkaConsumer setTopicName(String topicName) {
        this.topicName = topicName;
        return this;
    }

    public String getTopicName() {
        return topicName;
    }

    public AbstractKafkaConsumer setShouldSeekToEnd(boolean shouldSeekToEnd) {
        this.shouldSeekToEnd = shouldSeekToEnd;
        return this;
    }

    public boolean shouldSeekToEnd() {
        return shouldSeekToEnd;
    }

    public void stopConsumer() {
        executor.shutdownNow();
    }

    public void resetCounter() {
        count.set(0);
    }

    public void runConsumer() {
        executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            consumer = createConsumer();
            count = new AtomicInteger(1);
            while (!executor.isShutdown()) {
                final ConsumerRecords<String, V> consumerRecords =
                        consumer.poll(Duration.of(5L, ChronoUnit.SECONDS));

                StringBuilder result = new StringBuilder();
                List<String> records = new ArrayList<>();
                consumerRecords.forEach(record -> {
                    result.append("================================ Count: ").append(count.getAndIncrement())
                            .append(" ================================================").append("\n");
                    getHeaders(result, record);
                    getKey(result, record);
                    getValue(result, records, record);
                });
                if (!records.isEmpty() && !executor.isShutdown()) {
//                    System.out.println(records);
                    recordsProxy.records(records);
                    records.clear();
                }
                consumer.commitAsync();
            }
            consumer.close();
        });
    }

    Properties getBaseConsumerProperties() {
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokerAddress());
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, shouldSeekToEnd() ? OffsetResetStrategy.LATEST.name().toLowerCase() :
                OffsetResetStrategy.EARLIEST.name().toLowerCase());
        return consumerProperties;
    }

    private void getValue(StringBuilder result, List<String> records, ConsumerRecord<String, V> record) {
        String recordString = getRecordString(record.value());
        if (recordString != null && !recordString.isEmpty()) {
            result.append("Value: ").append(logJson(recordString)).append("\n");
            records.add(result.toString());
            result.setLength(0);
        }
    }

    private void getKey(StringBuilder result, ConsumerRecord<String, V> record) {
        String key = record.key();
        if (key != null && !key.isEmpty()) {
            result.append("Key: ").append(key).append("\n");
        }
    }

    private void getHeaders(StringBuilder result, ConsumerRecord<String, V> record) {
        Map<String, String> headersMap = new HashMap<>();
        record.headers().forEach(h -> headersMap.put(h.key(), new String(h.value())));
        if (!headersMap.isEmpty()) {
            result.append("Headers: ");
            headersMap.forEach((s, s2) -> result.append(s).append(":").append(s2).append(","));
            result.append("\n");
        }
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
