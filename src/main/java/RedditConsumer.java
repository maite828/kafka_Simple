import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class RedditConsumer {
    public static final String TOPIC_NAME = "reddit-posts";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "reddit-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(record -> processRedditPost(record.value()));
        }
    }

    private static void processRedditPost(String jsonMessage) {
        JSONObject redditPost = new JSONObject(jsonMessage);
        String title = redditPost.getString("title");
        String subreddit = redditPost.getString("subreddit");
        String url = redditPost.getString("url");

        // Puedes realizar cualquier procesamiento adicional aquí
        System.out.println("Received Reddit Post:");
        System.out.println("Title: " + title);
        System.out.println("Subreddit: " + subreddit);
        System.out.println("URL: " + url);
        System.out.println("----------------------");
    }
}
