package producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import static org.junit.Assert.*;

public class TransactionProducerTest {
    @Test
    public void newTransactionTest() {
        ProducerRecord<String, String> record = TransactionProducer.newTransaction("john");
        System.out.println(record.value());

        // Test the topic and the key
        assertEquals("transactions", record.topic());
        assertEquals("john", record.key());

        // Test the name of the user transaction
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode node = mapper.readTree(record.value());
            assertEquals(node.get("name").asText(), "john");
            assertNotEquals(node.get("amount").asInt(), null);
            assertNotEquals(node.get("time").asText(), null);
            assertTrue("Amount should be less than 100 and greater than 0", node.get("amount").asInt() > 0 && node.get("amount").asInt() < 100);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
