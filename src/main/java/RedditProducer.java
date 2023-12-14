import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.slf4j.Logger;

import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class RedditProducer {

    public static final String TOPIC_NAME = "rawtweets";
    private static final String PUSHSHIFT_API_URL = "https://api.pushshift.io/reddit/search/submission";

    private static final Logger logger = LoggerFactory.getLogger(RedditProducer.class); // Initialize logger

    public static void main(String[] args) throws IOException {

        Properties props = new Properties();
        props.put("acks", "1");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

        while (true) {
            try {
                String response = getRedditPosts(); // Attempt to fetch data from the PushShift API
                parseAndPublishRedditPosts(response, producer);
            } catch (IOException e) {
                logger.error("Error fetching Reddit posts:", e); // Log error message
            }

                try {
                    Thread.sleep(10000); // Sleep for 10 seconds before fetching new posts
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static String getRedditPosts() throws IOException {
        Connection connection = Jsoup.connect(PUSHSHIFT_API_URL) // Connect to the PushShift API
                .method(Connection.Method.GET); // Set the HTTP method to GET

        logger.info("Fetching Reddit posts from {}", PUSHSHIFT_API_URL); // Log fetching information

        return connection.execute().body(); // Execute the request and return the response body
    }

    private static void parseAndPublishRedditPosts(String response, KafkaProducer<String, String> producer) {
        JSONObject jsonObject = new JSONObject(response); // Parse the JSON response
        JSONArray postsArray = jsonObject.getJSONArray("data");

        for (int i = 0; i < postsArray.length(); i++) {
            JSONObject postObject = postsArray.getJSONObject(i);
            String title = postObject.getString("title");
            String selftext = postObject.getString("selftext");
            String subreddit = postObject.getString("subreddit");
            String url = postObject.getString("url");

            // Create a JSON object to represent the Reddit post
            JSONObject redditPost = new JSONObject();
            redditPost.put("title", title);
            redditPost.put("selftext", selftext);
            redditPost.put("subreddit", subreddit);
            redditPost.put("url", url);

            // Send the JSON representation of the Reddit post to the Kafka topic
            producer.send(new ProducerRecord<>(TOPIC_NAME, subreddit, redditPost.toString()));
        }
    }
}