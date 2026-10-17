package data02;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @author: xusong
 * @create:2025-12-21 11:04:20
 **/
public class Demo11 {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Integer>> dataStreamSource = env.fromElements(
                Tuple2.of("篮球", 1),
                Tuple2.of("篮球", 2),
                Tuple2.of("篮球", 3),
                Tuple2.of("足球", 3),
                Tuple2.of("足球", 2),
                Tuple2.of("足球", 3)
        );

        dataStreamSource.keyBy (tuple2-> tuple2.f0).print();

        //2. source-加载数据
        //3. transformation-数据处理转换
        //4. sink-数据输出

        //5. execute-执行
        env.execute();
    }
}