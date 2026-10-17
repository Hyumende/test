package STUDY;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;


import java.time.Duration;
import java.util.HashSet;
import java.util.Properties;
public class Kafka_CustomConsum_topic {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"root:9092");

        // 字段反序列化   key 和  value
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test_123");

        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        HashSet<String> set = new HashSet<>();

        set.add("test");
        //添加消费主题topic
        kafkaConsumer.subscribe(set);

        while (true){
            //poll方法拉去消息
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.topic()+"->"+record.partition()+"->"+record.value());
            }
        }

    }
}
