package streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class BankBalanceApp {
    public static void main(String args[]) {
        new BankBalanceApp().run();
    }

    public void run() {

    }

    public Properties createProps() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-app");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return props;
    }

    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> bankTransactions = builder.stream("transactions");

        /*KGroupedStream<String, String> bankBalance =  bankTransactions
                .groupByKey()
                .aggregate(
                        () -> 0L,
                        (aggKey, newValue, aggValue) -> aggValue + ObjectMapper.readTree(aggValue).get("amount").asInt(),
                        Serdes.Integer(),
                        "aggregated-stream-store"
                );*/

        return builder.build();
    }
}
