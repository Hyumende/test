package date05;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.RuntimeExecutionMode;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @author: xuesong
 * @create:2025-12-26 20:50:03
 **/
public class State {
    public static void main(String[] args) throws Exception {
        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        DataStream<Tuple2<String, Long>> tupleDS = env.fromElements(
                Tuple2.of("北京", 1L),
                Tuple2.of("上海", 2L),
                Tuple2.of("北京", 6L),
                Tuple2.of("上海", 8L),
                Tuple2.of("北京", 3L),
                Tuple2.of("上海", 4L),
                Tuple2.of("北京", 7L)
        );
        tupleDS.keyBy(tuple2 -> tuple2.f0).map(new RichMapFunction<Tuple2<String, Long>, Tuple2<String,Long>>() {

            ValueState<Long> maxValueState ;

            @Override
            public Tuple2<String, Long> map(Tuple2<String, Long> value) throws Exception {
                Long f1 = value.f1;
                if(maxValueState.value() == null || f1.longValue()>maxValueState.value()){
                    maxValueState.update(f1);
                }
                return Tuple2.of(value.f0,maxValueState.value());
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<Long>("valueState",Long.class);
                maxValueState = getRuntimeContext().getState(stateDescriptor);

            }

            @Override
            public void close() throws Exception {
                maxValueState.clear();
            }
        }).print();

        //2. source-加载数据
        //3. transformation-数据处理转换
        //4. sink-数据输出

        //5. execute-执行
        env.execute();
    }
}
