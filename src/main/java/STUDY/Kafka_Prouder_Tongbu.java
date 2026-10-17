package STUDY;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Kafka_Prouder_Tongbu {
    public static void main(String[] args)  throws ExecutionException, InterruptedException {
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
            ProducerRecord<String,String> producerRecord = new ProducerRecord<String,String>("first","root:"+i);
            //发送消息
            kafkaProducer.send(producerRecord).get();
        }
        kafkaProducer.close();
    }

}
