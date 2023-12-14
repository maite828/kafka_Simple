
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import java.util.Properties;

public class RedditConsumerKTable {
    public static void main(String[] args) {
        Properties consumerProps = new Properties();
        consumerProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "reddit-post-count-consumer");
        consumerProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        consumerProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        // Consumir 'reddit-post-count' como KTable
        KTable<String, Long> redditPostCountTable = builder.table(
                "processed-reddit-posts",
                Consumed.with(Serdes.String(), Serdes.Long())
        );

        // Imprimir el KTable por consola
        redditPostCountTable.toStream().print(Printed.<String, Long>toSysOut().withLabel("Reddit Post Count Table"));

        KafkaStreams consumerStreams = new KafkaStreams(builder.build(), consumerProps);
        consumerStreams.start();

        // Agrega un cierre de la aplicación
        Runtime.getRuntime().addShutdownHook(new Thread(consumerStreams::close));
    }
}

