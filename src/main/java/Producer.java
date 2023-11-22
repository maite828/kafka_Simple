import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class Producer {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("buffer.memory", 33554432);

        KafkaProducer<String, String> prod = new KafkaProducer<>(props);
        String topic = "rawtweets";
        int count = 0;

        while (true){
            try {
                Thread.sleep(1000);
                count ++;
                prod.send(new ProducerRecord<>(topic,0,"testKey", String.valueOf(count)));
            } catch (InterruptedException e) {
                e.printStackTrace();
                prod.close();
            }
        }
    }
}