import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class RedditConsumerStreams {
    public static void main(String[] args) {
        Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "reddit-streams");
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, org.apache.kafka.common.serialization.Serdes.String().getClass().getName());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, org.apache.kafka.common.serialization.Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        // Lee los mensajes desde el tema
        KStream<String, String> redditPosts = builder.stream("reddit-posts");

        // Utiliza peek para imprimir todo el contenido del KStream
        redditPosts.peek((key, value) -> System.out.println("Received record: key=" + key + ", value=" + value));

        // Procesa los mensajes (puedes aplicar tus lógicas de procesamiento aquí)
        redditPosts
                .groupByKey()
                .count(Materialized.as("reddit-post-count"))
                .toStream()
                .to("processed-reddit-posts", Produced.with(org.apache.kafka.common.serialization.Serdes.String(), org.apache.kafka.common.serialization.Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsProps);
        streams.start();

        // Agrega un cierre de la aplicación
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
