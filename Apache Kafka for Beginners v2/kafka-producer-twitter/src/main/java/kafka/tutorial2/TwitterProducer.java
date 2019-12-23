package kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    // Twitter producer settings
    private final String CONSUMER_KEY = "RvsUruA5d57WtIvMPtanSHe6j";
    private final String CONSUMER_SECRET = "7c5yX4k0baDhGHiM7lqkcfplA4UClb1NPHThOxUCNza2JUjvTR";
    private final String TOKEN = "405520228-zBnmIejxQ8GqiZZi3UXjAHKSty8Wfd6aDA082mGt";
    private final String TOKEN_SECRET = "v9P8Pb6InW6Nr5lkaRG2sHrZKdthA8MMq2qzyNYOUaYZh";

    // Terms that we want to get tweets from
    private List<String> terms = Lists.newArrayList("kafka", "bitcoin", "Real Madrid", "The Witcher");

    private Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    public TwitterProducer() {}

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    /**
     * Run the logic of the app
     */
    public void run() {
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1);

        // Create a Twitter client
        Client client = createTwitterClient(msgQueue);
        client.connect();

        // Create a Kafka Producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // Add a shutdown hook - This logic will happen when the application is finished or stopped.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping application...");
            logger.info("Shutting down client from twitter...");
            client.stop();

            logger.info("Closing producer...");
            producer.close();
            logger.info("Done!");
        }));

        // Loop to send tweets to Kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;

            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }

            if (msg != null) {
                logger.info("Message received: " + msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null)
                            logger.error("Something bad happened", e);
                    }
                });
            }
        }

        logger.info("End of application");
    }

    /**
     * Creates the Twitter Client
     * @param msgQueue - List of terms we want to get tweets from
     * @return hosebirdClient - The twitter client
     */
    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        /**
         * This configuration is described in https://github.com/twitter/hbc
         */

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, TOKEN_SECRET);

        // Create the client
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }

    /**
     * Creates the Kafka Producer
     * @return - the Producer
     */
    public KafkaProducer<String, String> createKafkaProducer() {
        // Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create safe Producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); //kafka 2.0 >= 1.1 so we can keep this as 5. Use 1 otherwise

        // High throughput (performance) producer (at the expense of a bit of latency ad CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); // 20 ms of delay in the messages delivery
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32 KB batch size

        // Create the producer and return it
        return new KafkaProducer<String, String>(properties);
    }
}
