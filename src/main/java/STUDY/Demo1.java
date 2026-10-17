package STUDY;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("root:9092,root2:9092,root3:9092")
                .setTopics("flink")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();

        DataStreamSource<String> DataStreamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),"KakfaSource");

        SingleOutputStreamOperator<Tuple2<String, Double>> map = DataStreamSource.map(new MapFunction<String, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(String s) throws Exception {

                JSONObject jsonObject = JSON.parseObject(s);
                String order_type = jsonObject.getString("order_type");
                Double orderAmt = jsonObject.getDouble("order_amt");
                return Tuple2.of(order_type, orderAmt);
            }
        });

        KeyedStream<Tuple2<String, Double>, String> KeyedStream = map.keyBy(s -> s.f0);

        KeyedStream.sum(1).print();

        env.execute();


    }/**/

}
