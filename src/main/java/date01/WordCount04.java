package date01;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCount04 {
    public static void main(String[] args) throws Exception {
        /**
         * 1. env-准备环境
         * 2. source-加载数据
         * 3. transformation-数据处理转换
         * 4. sink-数据输出
         * 5. execute-执行
         */
        /**
         * 传参版
         * */
        //env环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //指定该任务是按照那个方式执行 BATCH Streaming automatic
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        //加载数据
        DataStreamSource<String> dataStreamSource = null;
        //参数数量要修改为2
        if(args.length == 2){
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            if(parameterTool.has("input")){
                String file = parameterTool.get("input");
                System.out.println(file);
                FileSource<String> fileSource = FileSource
                        .forRecordStreamFormat(
                                new TextLineInputFormat(),
                                new Path(file)
                        ).build();
                dataStreamSource =env.fromSource(fileSource, WatermarkStrategy.noWatermarks(),"fileSource");
            }
        }
        else
        {
                dataStreamSource = env.fromElements("spark flink kafka", "spark sqoop flink", "kakfa hadoop flink");
        }
        //通过各种算子进行计算,通过匿名内部类实现参数接口，最后收集器收集扔出为Streaming流
        SingleOutputStreamOperator<String> flatMap = dataStreamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                String[] s = line.split("\\s+");
                for (String word : s) {
                    collector.collect(word);
                }
            }
        });
        // hello转化为元组（）
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = flatMap.map(new MapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        });
        //按照Key值分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = map.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {

            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });
        //按照下标第二个元素进行计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum(1);
        sum.print();

        env.execute();

    }
}
