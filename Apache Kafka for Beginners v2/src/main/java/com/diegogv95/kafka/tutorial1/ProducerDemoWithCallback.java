package com.diegogv95.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    final static String BOOTSTRAP_SERVER = "localhost:9092";
    final static String KEY_SERIALIZER = StringSerializer.class.getName();
    final static String VALUE_SERIALIZER = StringSerializer.class.getName();

    static Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main (String[] args) {
        // Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER); // ProducerConfig.BOOTSTRAP_SERVERS_CONFIG = "bootstrap.server"
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER); // ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG = "key.serializer"
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER); // ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG = "value.serializer"

        // Create the Producer with the properties declared above
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            // Create a Producer record
            final ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello world " + Integer.toString(i)); // key, value

            // Send data - asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // Execute every time a record is successfully sent or and exception is thrown
                    if (e == null) {
                        // The record was successfully sent
                        logger.info("Received new metadata: \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            });
        }

        // Flush data
        producer.flush();
        // Flush data and close producer
        producer.close();
    }
}
