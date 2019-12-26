package com.diegogv95.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {
    final static String BOOTSTRAP_SERVER = "localhost:9092";
    final static String KEY_SERIALIZER = StringSerializer.class.getName();
    final static String VALUE_SERIALIZER = StringSerializer.class.getName();

    static Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

    public static void main (String[] args) throws ExecutionException, InterruptedException {
        // Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER); // ProducerConfig.BOOTSTRAP_SERVERS_CONFIG = "bootstrap.server"
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER); // ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG = "key.serializer"
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER); // ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG = "value.serializer"

        // Create the Producer with the properties declared above
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            // Create a Producer record
            String topic = "first_topic";
            String value = "Hello world " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value); // key, value

            logger.info("Key: " + key); // log the key
            // id_0 is going to partition 1
            // id_1 is going to partition 0
            // id_2 is going to partition 2
            // id_3 is going to partition 0
            // id_4 is going to partition 2
            // id_5 is going to partition 2
            // id_6 is going to partition 0
            // id_7 is going to partition 2
            // id_8 is going to partition 1
            // id_9 is going to partition 2

            // This location will always be the same, does not matter how many times you lunch the application.
            // The records with the same key will always go to the same partition - IMPORTANT!

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
            }).get(); // block the .send() to make it synchronous - do not do this in production!
        }

        // Flush data
        producer.flush();
        // Flush data and close producer
        producer.close();
    }
}
