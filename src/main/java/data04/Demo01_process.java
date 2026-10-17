package data04;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @author: xuesong
 * @create:2025-12-24 21:58:05
 **/
public class Demo01_process {
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
        dataStreamSource.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String line) throws Exception {
               return Tuple2.of(line,1);
            }
        }).keyBy(tuple2 -> tuple2.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                // 第一个泛型是输入数据的类型，第二个泛型是返回值类型   第三个是key 的类型， 第四个是窗口对象
                        .process(new ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {



                            @Override
                            public void process(String key, ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>.Context context, Iterable<Tuple2<String, Integer>> input, Collector<String> out) throws Exception {
                                int sum = 0;
                                for (Tuple2<String, Integer> tuple2 : input) {
                                     sum+= tuple2.f1;
                                }
                                TimeWindow window = context.window();
                                long start = window.getStart();
                                long end = window.getEnd();
                                String starttime = DateFormatUtils.format(start,"yyyy-mm-dd HH:mm:ss");
                                String endtime = DateFormatUtils.format(end,"yyyy-mm-dd HH:mm:ss");
                                StringBuffer stringBuffer = new StringBuffer();
                                stringBuffer.append(starttime).append(":").append(endtime).append(",").append(key).append(",").append(sum);

                                out.collect(stringBuffer.toString());
                            }
                        }).print();
        //3. transformation-数据处理转换
        //4. sink-数据输出

        //5. execute-执行
        env.execute();
    }
}
