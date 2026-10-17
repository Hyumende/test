package data02;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @author: xusong
 * @create:2025-12-21 13:41:29
 **/
public class Demo16_SideOutPut {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // 将一个流，拆分为多个流
        DataStreamSource<Long> dataStreamSource = env.fromSequence(0, 100);
        // 将这个流分为奇数和偶数流
        // 定义两个标签
        OutputTag<Long> tag_even = new OutputTag<Long>("偶数", TypeInformation.of(Long.class));
        OutputTag<Long> tag_odd = new OutputTag<Long>("奇数", TypeInformation.of(Long.class));
        //3. transformation-数据处理转换
        //4. sink-数据输出
        SingleOutputStreamOperator<Long> process = dataStreamSource.process(new ProcessFunction<Long, Long>() {
            @Override
            public void processElement(Long aLong, ProcessFunction<Long, Long>.Context context, Collector<Long> collector) throws Exception {
                if (aLong % 2 == 0) {
                    context.output(tag_even, aLong);
                } else {
                    context.output(tag_odd, aLong);
                }
            }
        });
        SideOutputDataStream<Long> evenStream = process.getSideOutput(tag_even);
        SideOutputDataStream<Long> oddStream = process.getSideOutput(tag_odd);
        evenStream.print("偶数");
        oddStream.print("奇数");
        //5. execute-执行
        env.execute();
    }
}