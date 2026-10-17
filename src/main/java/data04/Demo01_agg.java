package data04;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @author: xuesong
 * @create:2025-12-25 21:04:24
 **/
public class Demo01_agg {
    public static final Tuple3[] ENGLISH = new Tuple3[] {
            Tuple3.of("class1", "张三", 100L),
            Tuple3.of("class1", "李四", 40L),
            Tuple3.of("class1", "王五", 60L),
            Tuple3.of("class2", "赵六", 20L),
            Tuple3.of("class2", "小七", 30L),
            Tuple3.of("class2", "小八", 50L)
    };
    public static void main(String[] args) throws Exception {
        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStreamSource<Tuple3<String,String,Long>> DataStreamSource = env.fromElements(ENGLISH);

        DataStreamSource.keyBy(tuple3 -> tuple3.f0).countWindow(3).apply(new WindowFunction<Tuple3<String, String, Long>, Tuple2<String, Double>, String, GlobalWindow>() {
            @Override
            public void apply(String key, GlobalWindow globalWindow, Iterable<Tuple3<String, String, Long>> iterable, Collector<Tuple2<String, Double>> collector) throws Exception {
                long sum = 0;
                int person = 0;
                for (Tuple3<String, String, Long> tuple3 : iterable) {
                    sum+=tuple3.f2;
                    person+=1;
                }
                collector.collect(Tuple2.of(key,sum*1.0/person));
            }
        }).print();

        //2. source-加载数据
        //3. transformation-数据处理转换
        //4. sink-数据输出

        //5. execute-执行
        env.execute();
    }
}
