package producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import payload.Transaction;
import serializer.TransactionSerializer;

import java.math.BigDecimal;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.IntStream;

import static common.KafkaUtil.TOPIC_NAME;


@Slf4j
public class ProducerAPI {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TransactionSerializer.class.getName());

        try (KafkaProducer<Integer, Transaction> producer = new KafkaProducer<>(properties)) {
            IntStream.range(0, 10)
                    .forEach(i -> {
                        Transaction transaction = Transaction.builder()
                                .userId((long) i)
                                .transactionId(UUID.randomUUID().toString())
                                .amount(BigDecimal.valueOf(10L * i))
                                .build();
                        ProducerRecord<Integer, Transaction> record = new ProducerRecord<>(TOPIC_NAME, i, transaction);
                        producer.send(record, ((metadata, e) -> {
                            if (e != null) {
                                e.printStackTrace();
                            } else {
                                log.info("Record published\nPartition:{}, Offset:{}, ", metadata.partition(), metadata.offset());
                            }
                        }));
                    });
        }
    }
}
