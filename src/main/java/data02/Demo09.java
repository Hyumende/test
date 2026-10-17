package data02;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.FileRecordFormat;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @author: xusong
 * @create:2025-12-21 01:07:18
 **/
public class Demo09 {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),new Path("datas/flatmap.log")
        ).build();
        DataStreamSource<String> dataStreamSource = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "flatmap算子");
        //2. source-加载数据
        //3. transformation-数据处理转换
        dataStreamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                String[] arr = line.split(",");
                String name = arr[0];
                for (int i = 1; i < arr.length; i++) {
                    String goodsName = arr[i];
                    System.out.println(arr[0]+"有"+arr[i]);
                }
            }
        }).print();
        //4. sink-数据输出

        //5. execute-执行
        env.execute();
    }
}