package com.diegogv95.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoWithGroups {
    // Declare the Consumer properties
    final static String BOOTSTRAP_SERVER = "localhost:9092";
    final static String KEY_DESERIALIZER = StringDeserializer.class.getName();
    final static String VALUE_DESERIALIZER = StringDeserializer.class.getName();
    final static String GROUP_ID = "my-fifth-application";
    final static String OFFSET_RESET = "earliest";
    final static String TOPIC = "first_topic";

    static Logger logger = LoggerFactory.getLogger(ConsumerDemoWithGroups.class);

    public static void main(String[] args) {
        // Create Consumer properties;
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER); // ProducerConfig.BOOTSTRAP_SERVERS_CONFIG = "bootstrap.server"
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KEY_DESERIALIZER); // ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG = "key.serializer"
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER); // ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG = "value.serializer"
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET); // earliest ->  you will read from the very beginning.
                                                                                       // latest -> you will read only the new records.
                                                                                       // none -> you won't read any messages.

        // Create the Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // Receive the data
        consumer.subscribe(Collections.singleton(TOPIC)); // You only want to connect to one topic
        // consumer.subscribe(Arrays.asList("first_topic", "second_topic")); -> You can connect to several topics

        // Poll for new data
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // 100 ms

            for (ConsumerRecord<String, String> record : records) {
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }
    }
}
