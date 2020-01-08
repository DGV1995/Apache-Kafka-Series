package kafka_streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class StreamsStarterApp {
    public static void main(String[] args) {
        // Create properties
        Properties properties = createProperties();

        // Create the topology
        Topology topology = createTopology();

        // Create the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(topology, properties);
        streams.start();
    }

    public static Properties createProperties() {
        Properties properties = new Properties();

        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "word-count");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.class.getName());

        return properties;
    }

    public static Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        // 1-Stream from kafka
        KStream<String, String> inputStream = builder.stream("word-count-input");
        // 2-Map values to lowercase
        KTable<String, Long> wordCountTable = inputStream
                .mapValues(textLine -> textLine.toLowerCase())
                // 3 - Flatmap values split by space
                .flatMapValues(textLine -> Arrays.asList(textLine.split(" ")))
                // 4 - Select key to apply a key (we discard the old key)
                .selectKey((key, word) -> word)
                // 5 - Group by key before aggregation
                .groupByKey()
                // 6 - Count occurences
                .count(Materialized.as("Counts"));

        // 7 - Write the results back to kafka
        wordCountTable.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }
}
