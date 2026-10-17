package data02;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @author: xusong
 * @create:2025-12-21 13:03:57
 **/
public class Demo15 {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds1 = env.fromElements("大数据", "JavaWeb", "鸿蒙开发");
        DataStreamSource<String> ds2 = env.fromElements("Bigdata", "SpringBoot", "JS");

        DataStream<String> dataStream = ds1.union(ds2);
        dataStream.print();
        DataStreamSource<Long> ds3 = env.fromSequence(1, 10);
        ConnectedStreams<String, Long> connectedStreams = ds1.connect(ds3);
        connectedStreams.map(new CoMapFunction<String, Long, String>() {
            @Override
            public String map1(String s) throws Exception {
                return s;
            }

            @Override
            public String map2(Long aLong) throws Exception {
                return aLong.toString();
            }
        }).print();

        ds1.connect(ds3).process(new CoProcessFunction<String, Long, String>() {

            @Override
            public void processElement1(String s, CoProcessFunction<String, Long, String>.Context context, Collector<String> collector) throws Exception {
                collector.collect(s);
            }

            @Override
            public void processElement2(Long aLong, CoProcessFunction<String, Long, String>.Context context, Collector<String> collector) throws Exception {
                collector.collect(aLong.toString());
            }
        }).print();

        //2. source-加载数据
        //3. transformation-数据处理转换
        //4. sink-数据输出

        //5. execute-执行
        env.execute();
    }
}