package STUDY;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class Kafka__Partition {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //设置kafka集群ip和接口
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("bootstrap.servers", "root:9092");

        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            ProducerRecord<String,String> producerRecord = new ProducerRecord<>(
                "test",1,"abc","good+"+i
            );
            kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.printf("消息发送给了%d分区\n",(recordMetadata.partition()));
                }
            });
        }
        kafkaProducer.close();
    }

}
