import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Application {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";
    private static final String TOPIC = "events";

    public static void main(String[] args) {
        String consumerGroup = "defaultConsumerGroup";
        if(args.length == 1){
            consumerGroup = args[0];
        }

        System.out.println("consumer group is: " + consumerGroup);
        Consumer<Long, String> kafkaConsumer = createKafkaConsumer(BOOTSTRAP_SERVERS, consumerGroup);
        consumeMessage(TOPIC, kafkaConsumer);
    }

    public static void consumeMessage(String topic, Consumer<Long, String> kafkaConsumer){
            kafkaConsumer.subscribe(Collections.singleton(topic));
            while (true){
                ConsumerRecords<Long, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
                if (consumerRecords.isEmpty()){

                }
                for(ConsumerRecord<Long, String> consumerRecord: consumerRecords){
                    System.out.println(String.format("Received record (key: %d, val: %s) in (partition: %d offset: %d)",
                            consumerRecord.key(), consumerRecord.value(), consumerRecord.partition(), consumerRecord.offset()));
                }

                kafkaConsumer.commitAsync();
            }


    }

    public static Consumer<Long, String> createKafkaConsumer(String bootstrapServers, String consumerGroup){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // better to commit manually:
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new KafkaConsumer<Long, String>(properties);

    }
}
