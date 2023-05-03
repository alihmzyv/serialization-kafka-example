package serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.SneakyThrows;
import lombok.experimental.FieldDefaults;
import org.apache.kafka.common.serialization.Deserializer;
import payload.Transaction;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class TransactionDeserializer implements Deserializer<Transaction> {
    ObjectMapper objectMapper = new ObjectMapper();

    @SneakyThrows
    @Override
    public Transaction deserialize(String topic, byte[] data) {
        return objectMapper.readValue(data, Transaction.class);
    }
}
