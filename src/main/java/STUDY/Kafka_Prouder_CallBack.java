package STUDY;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class Kafka_Prouder_CallBack {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //设置kafka集群ip和接口
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("bootstrap.servers", "root:9092");

        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 1; i++) {
            ProducerRecord<String,String> producerRecord = new ProducerRecord<String,String>("first","hello world"+i);
            kafkaProducer.send(producerRecord, new Callback() {
                //回调函数，返回分区、偏移量、主题topic信息。
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception==null){
                        System.out.println(metadata.partition());
                        System.out.println(metadata.offset());
                        System.out.println(metadata.topic());
                    }else{
                        System.out.println("发生了异常"+exception.getMessage());
                    }
                }
            });
        }
        kafkaProducer.close();
    }

}
