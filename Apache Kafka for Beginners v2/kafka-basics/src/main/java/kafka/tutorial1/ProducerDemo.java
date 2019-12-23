package kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    final static String BOOTSTRAP_SERVER = "localhost:9092";
    final static String KEY_SERIALIZER = StringSerializer.class.getName();
    final static String VALUE_SERIALIZER = StringSerializer.class.getName();

    public static void main (String[] args) {
        // Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER); // ProducerConfig.BOOTSTRAP_SERVERS_CONFIG = "bootstrap.server"
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER); // ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG = "key.serializer"
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER); // ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG = "value.serializer"

        // Create the Producer with the properties declared above
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        // Create a Producer record
        ProducerRecord<String,String> record = new ProducerRecord<String, String>("first_topic", "Hello world!"); // key, value

        // Send data - asynchronous
        producer.send(record);

        // Flush data
        producer.flush();
        // Flush data and close producer
        producer.close();
    }
}
