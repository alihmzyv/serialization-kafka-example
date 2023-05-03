package consumer;

import common.KafkaUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import payload.Transaction;
import serializer.TransactionDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class ConsumerAPI {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TransactionDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "transaction-cons-group-1");

        KafkaConsumer<Integer, Transaction> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton(KafkaUtil.TOPIC_NAME));
        try (consumer) {
            while (true) {
                ConsumerRecords<Integer, Transaction> records = consumer.poll(Duration.ofSeconds(2));
                records.forEach(record -> {
                    Transaction value = record.value();
                    int partition = record.key();
                    log.info("Record consumed");
                    log.info("Value: {}, Partition: {}", value, partition);
                });
            }
        }
    }
}
