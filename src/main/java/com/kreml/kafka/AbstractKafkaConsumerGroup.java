package com.kreml.kafka;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import javafx.application.Platform;
import javafx.collections.ObservableList;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.CharArrayReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractKafkaConsumerGroup<V> implements OnCancelListener {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private String brokerAddresses;
    private String topicName;
    private boolean shouldSeekToEnd;
    private AtomicInteger count = new AtomicInteger(1);
    private AtomicBoolean proceed = new AtomicBoolean(true);
    private AtomicInteger onCancelRunTimes = new AtomicInteger();
    private ObservableList<String> observableList;
    private ExecutorService executor;
    private Runnable onCancel;
    private List<KafkaConsumer<String, V>> consumerList = new ArrayList<>();
    private static UUID groupId = UUID.randomUUID();
    private int consumersCount;

    abstract KafkaConsumer<String, V> createConsumer();

    abstract String getRecordString(V value);

    AbstractKafkaConsumerGroup(ObservableList<String> observableList) {
        this.observableList = observableList;
    }

    public String getBrokerAddresses() {
        return brokerAddresses;
    }

    public AbstractKafkaConsumerGroup<V> setBrokerAddresses(String brokerAddresses) {
        this.brokerAddresses = brokerAddresses;
        return this;
    }

    public AbstractKafkaConsumerGroup<V> setTopicName(String topicName) {
        this.topicName = topicName;
        return this;
    }

    public String getTopicName() {
        return topicName;
    }

    public AbstractKafkaConsumerGroup<V> setShouldSeekToEnd(boolean shouldSeekToEnd) {
        this.shouldSeekToEnd = shouldSeekToEnd;
        return this;
    }

    public boolean shouldSeekToEnd() {
        return shouldSeekToEnd;
    }

    public AbstractKafkaConsumerGroup<V> setConsumersCount(int consumersCount) {
        this.consumersCount = consumersCount;
        return this;
    }

    public void stopConsumer() {
        proceed.set(false);
        consumerList.forEach(stringVKafkaConsumer -> {
            if (stringVKafkaConsumer != null) {
                stringVKafkaConsumer.wakeup();
            }
        });
        executor.shutdown();
    }

    public void resetList() {
        count.set(1);
        observableList.clear();
    }

    public void runConsumer() {
        executor = Executors.newFixedThreadPool(consumersCount);
        count = new AtomicInteger(1);
        proceed.set(true);
        onCancelRunTimes.set(0);
        for (int i = 0; i < consumersCount; i++) {
            executor.submit(getFetchingRunnable());
        }
    }

    private Runnable getFetchingRunnable() {
        return () -> {
            KafkaConsumer<String, V> consumer = createConsumer();
            consumerList.add(consumer);
            try {
            while (proceed.get()) {
                    final ConsumerRecords<String, V> consumerRecords =
                            consumer.poll(Duration.ofMillis(500L));

                    StringBuilder result = new StringBuilder();
                    List<String> resultList = new ArrayList<>();
                    consumerRecords.forEach(record -> {
                        result.append("================================ Count: ").append(count.getAndIncrement())
                                .append(" ================================================").append("\n");
                        getHeaders(result, record);
                        getKey(result, record);
                        getRecordValue(result, record);
                        resultList.add(result.toString());
                        result.setLength(0);
                    });
                    if (!resultList.isEmpty() && proceed.get()) {
                        List<String> tempList = new ArrayList<>(resultList);
                        Platform.runLater(() -> {
                            observableList.addAll(tempList);
                        });
                        consumer.commitAsync();
                    }
            }
            } catch (WakeupException e) {
                if (proceed.get()) throw e;
            } finally {
                consumer.close();
                logger.info("Consumer is stopped.");
                runOnCancel();
            }
        };
    }

    private void runOnCancel() {
        if (onCancelRunTimes.incrementAndGet() == consumersCount) {
            Platform.runLater(onCancel);
        }
    }

    Properties getBaseConsumerProperties() {
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokerAddresses());
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, getUniqueGroupID());
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, shouldSeekToEnd() ? OffsetResetStrategy.LATEST.name().toLowerCase() :
                OffsetResetStrategy.EARLIEST.name().toLowerCase());
        return consumerProperties;
    }

    private String getUniqueGroupID() {
        return groupId.toString();
    }

    private void getRecordValue(StringBuilder result, ConsumerRecord<String, V> record) {
        String recordString = getRecordString(record.value());
        if (recordString != null && !recordString.isEmpty()) {
            result.append("Value: ").append(logJson(recordString)).append("\n");
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
        String result;
        try{
            Gson gson = new GsonBuilder().setPrettyPrinting().setLenient().create();
            JsonReader r = new JsonReader(new CharArrayReader(JSONString.toCharArray()));
            r.setLenient(true);
            JsonParser jp = new JsonParser();
            JsonElement je = jp.parse(r);
            result = gson.toJson(je);
        } catch (Throwable t) {
            result = JSONString;
        }
        return result;
    }

    @Override
    public void setOnCancelled(Runnable runnable) {
        onCancel = runnable;
    }
}
