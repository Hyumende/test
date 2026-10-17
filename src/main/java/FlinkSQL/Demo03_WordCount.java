package FlinkSQL;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @author: xuesong
 * @create:2025-12-31 20:36:38
 **/
public class Demo03_WordCount {
    public static void main(String[] args) throws Exception {
        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);


        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.154.100:9092")
                .setTopics("flink")
                .setGroupId("flink-kafka-test")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest())
/*                // 增加一些配置，例如：设置心跳间隔和会话超时
                .setProperty("heartbeat.interval.ms", "30000")
                .setProperty("session.timeout.ms", "60000")
                // 设置分区发现间隔，以便动态发现新分区
                .setProperty("partition.discovery.interval.ms", "10000")*/
                .build();

        DataStreamSource<String> dataStreamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");

        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] s1 = s.split(" ");
                for (String word : s1) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });

        //创建虚拟表
        tableEnvironment.createTemporaryView("mytable",tuple2SingleOutputStreamOperator,$("f0").as("word"),$("f1").as("num"));
        //把流变成表
        Table table = tableEnvironment.sqlQuery("select word,sum(num) from mytable group by word");
        //打印表结构
        table.printSchema();
        //把表转化为流打印
        tableEnvironment.toChangelogStream(table).print();


        //2. source-加载数据
        //3. transformation-数据处理转换
        //4. sink-数据输出

        //5. execute-执行
        env.execute();
    }
}
