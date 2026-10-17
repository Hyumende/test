package data02;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @author: xusong
 * @create:2025-12-21 11:18:46
 **/
public class Demo13 {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),new Path("datas/a.log")
        ).build();
        DataStreamSource<String> dataStreamSource = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "reduce算子");

        dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String,Integer>> value) throws Exception {
                String[] arr = line.split(" ");
                value.collect(Tuple2.of(arr[0],1));
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.f0;
            }
        }).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> tuple1, Tuple2<String, Integer> tuple2) throws Exception {
                return Tuple2.of(tuple1.f0,tuple1.f1+tuple2.f1);
            }
        }).print();

        //2. source-加载数据
        //3. transformation-数据处理转换
        //4. sink-数据输出

        //5. execute-执行
        env.execute();
    }
}