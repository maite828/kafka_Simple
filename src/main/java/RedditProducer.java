import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Properties;

public class RedditProducer {
    public static final String TOPIC_NAME = "reddit-posts";
    private static final String REDDIT_API_URL = "https://api.reddit.com/r/popular";

    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            while (true) {
                try {
                    String response = getRedditPosts();
                    parseAndPublishRedditPosts(response, producer);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static String getRedditPosts() throws IOException {
        HttpClient httpClient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(REDDIT_API_URL);
        httpGet.setHeader("Accept", "application/json");
        httpGet.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36");

        HttpResponse response = httpClient.execute(httpGet);

        return EntityUtils.toString(response.getEntity());
    }

    private static void parseAndPublishRedditPosts(String response, KafkaProducer<String, String> producer) {
        JSONObject jsonObject = new JSONObject(response);
        JSONArray postsArray = jsonObject.getJSONObject("data").getJSONArray("children");
        for (int i = 0; i < postsArray.length(); i++) {
            JSONObject postObject = postsArray.getJSONObject(i).getJSONObject("data");
            String title = postObject.getString("title");
            String subreddit = postObject.getString("subreddit");
            String url = postObject.getString("url");

            JSONObject redditPost = new JSONObject();
            redditPost.put("title", title);
            redditPost.put("subreddit", subreddit);
            redditPost.put("url", url);

            producer.send(new ProducerRecord<>(TOPIC_NAME, subreddit, redditPost.toString()));
        }
    }
}
