package com.diegogv95.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    // Declare the Consumer properties
    final static String BOOTSTRAP_SERVER = "localhost:9092";
    final static String KEY_DESERIALIZER = StringDeserializer.class.getName();
    final static String VALUE_DESERIALIZER = StringDeserializer.class.getName();
    final static String GROUP_ID = "my-sixth-application";
    final static String OFFSET_RESET = "earliest";
    final static String TOPIC = "first_topic";

    private static Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    // Constructor
    private ConsumerDemoWithThread() {}

    private void run() {
        // Latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // Create the consumer runnable
        logger.info("Creating the consumer thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(latch);

        // Start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            // Wait all the time until the application is over
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public static class ConsumerRunnable implements Runnable {
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        // Constructor
        public ConsumerRunnable(CountDownLatch latch) {
            this.latch = latch;

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
            consumer = new KafkaConsumer<String, String>(properties);
            // Receive the data
            consumer.subscribe(Collections.singleton(TOPIC));
        }

        @Override
        public void run() {
            // Poll for new data
            try {
                while(true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // 100 ms
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal");
            } finally {
                // Close our Consumer
                consumer.close();
                // Tell our main code we are done with the Consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            // The wakeup() method is a special method to interrupt consumer.poll()
            // It will throw the exception WakeUpException
            consumer.wakeup();
        }
    }
}
