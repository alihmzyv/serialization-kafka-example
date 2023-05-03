package serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.apache.kafka.common.serialization.Serializer;
import payload.Transaction;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class TransactionSerializer implements Serializer<Transaction> {
    ObjectMapper objectMapper = new ObjectMapper();

    @SneakyThrows
    @Override
    public byte[] serialize(String topic, Transaction data) {
        return objectMapper.writeValueAsBytes(data);
    }
}
