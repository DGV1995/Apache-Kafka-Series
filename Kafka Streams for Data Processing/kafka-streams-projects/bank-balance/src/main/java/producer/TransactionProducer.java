package producer;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class TransactionProducer {
    Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    public static void main(String args[]) {
        new TransactionProducer().run();
    }

    public TransactionProducer() {}

    public void run() {
        KafkaProducer<String, String> producer = createProducer();

        int i = 0;
        while(true) {
            logger.info("Producing batch: " + Integer.toString(i));
            try {
                producer.send(newTransaction("john"));
                Thread.sleep(100);
                producer.send(newTransaction("diego"));
                Thread.sleep(100);
                producer.send(newTransaction("alicia"));
                Thread.sleep(100);

                i++;
            } catch(InterruptedException e) {
                break;
            }
        }

        producer.close();
    }

    public KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // Ensure we do not push duplicates
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // Strongest producing guarantee
        props.put(ProducerConfig.RETRIES_CONFIG, "3");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "1");

        return new KafkaProducer<String, String>(props);
    }

    public static ProducerRecord<String, String> newTransaction(String name) {
        // Create an empty json object
        ObjectNode transaction = JsonNodeFactory.instance.objectNode();
        // Get amount and time values
        int amount = ThreadLocalRandom.current().nextInt(0, 100);
        Instant time = Instant.now();
        // Write the data to the json document
        transaction.put("name", name);
        transaction.put("amount", amount);
        transaction.put("time", time.toString());

        return new ProducerRecord<String, String>("bank-transactions", name, transaction.toString());
    }
}
