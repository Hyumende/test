package STUDY;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class Kafka_Prouder {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //设置kafka集群ip和接口
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("bootstrap.servers", "root:9092");

        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 6; i++) {
            //生产消息
            ProducerRecord<String,String> producerRecord = new ProducerRecord<String,String>("first","这是第"+i+"条kafka消息");
            //发送消息
            kafkaProducer.send(producerRecord);
        }
        kafkaProducer.close();
    }

}
