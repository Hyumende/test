package data03;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.checkerframework.checker.units.qual.K;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @author: xuesong
 * @create:2025-12-24 21:06:33
 **/
public class Demo04 {
    public static void main(String[] args) throws Exception {
        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
        KafkaSource<String> kafkaSource  = KafkaSource.<String>builder().setBootstrapServers("root:9092")
                .setTopics("flink")
                .setGroupId("flink-kafak-test")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();
        DataStreamSource<String> dataStreamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafak source");
        //2. source-加载数据
        dataStreamSource.map(new MapFunction<String, Tuple2<Integer,Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(String line) throws Exception {
                String[] arr = line.split(",");
                return Tuple2.of(Integer.valueOf(arr[0]),Integer.valueOf(arr[1]));
            }
        }).keyBy((KeySelector<Tuple2<Integer, Integer>, Integer>) integerIntegerTuple2 -> integerIntegerTuple2.f0)
                //统计十秒内的数据
               // .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .window(SlidingProcessingTimeWindows.of(Time.minutes(1),Time.seconds(30)))
                .sum(1).print();
        //3. transformation-数据处理转换
        //4. sink-数据输出

        //5. execute-执行
        env.execute();
    }
}
