import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import org.apache.kafka.streams.kstream.Printed;

import java.util.Collections;
import java.util.Properties;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public

class RedditConsumer {

    public static ObjectMapper objectMapper = new ObjectMapper();
    public final static String TOPIC_NAME = "rawtweets"; // Corrected topic name

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "reddithashtagscounterappkk1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());


        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> redditPosts = builder.stream(TOPIC_NAME); // Corrected topic name

        redditPosts.flatMapValues(value -> {
                    JsonNode root;
                    try {
                        JsonParser parser = objectMapper.getFactory().createParser(value);
                        parser.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
                        root = parser.readValueAsTree();

                        if (!root.isValueNode()) {
                            return Collections.emptyList();
                        }

                        JsonNode selftextNode = root.path("selftext");
                        if (selftextNode != null && !selftextNode.toString().equals("null")) {
                            // Extract hashtags from the selftext field
                            Pattern hashTagPattern = Pattern.compile("#\\w+");
                            Matcher matcher = hashTagPattern.matcher(selftextNode.asText());
                            if (matcher.find()) {
                                return Collections.singletonList(matcher.group());
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return Collections.emptyList();
                })
                .groupBy((key, value) -> value)
                .count()
                .toStream()
                .print(Printed.toSysOut());
        Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
    }

    public

    static String getHashtags(String input)

    {
        JsonNode root;
        try {
            root = objectMapper.readTree(input);
            JsonNode hashtagsNode = root.path("entities").path("hashtags"); // Extract hashtags from "entities" field
            if (!hashtagsNode.toString().equals("[]")) {
                return hashtagsNode.get(0).path("text").asText();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }
}