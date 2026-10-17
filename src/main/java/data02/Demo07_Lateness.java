package data02;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class Demo07_Lateness {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);



        DataStreamSource<OrderInfo> dataStreamSource = env.addSource(new MySource());

        OutputTag<OrderInfo> sideOutput = new OutputTag<OrderInfo>("side output"){};

        SingleOutputStreamOperator<OrderInfo> orderInfoSingleOutputStreamOperator = dataStreamSource
        .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(
        new SerializableTimestampAssigner<OrderInfo>() {
            @Override
            public long extractTimestamp(OrderInfo orderInfo, long l) {
                return orderInfo.getTimeStamp();
            }
        }));

        SingleOutputStreamOperator<Object> resultStream = orderInfoSingleOutputStreamOperator
                .keyBy(order -> order.getUid())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(10))
                .sideOutputLateData(sideOutput)
                .apply(new WindowFunction<OrderInfo, Object, Integer, TimeWindow>() {
                    @Override
                    public void apply(Integer integer, TimeWindow timeWindow, Iterable<OrderInfo> iterable, Collector<Object> collector) throws Exception {

                        long start = timeWindow.getStart();
                        long end = timeWindow.getEnd();
                        String startStr = DateFormatUtils.format(start, "HH:mm:ss");
                        String endStr = DateFormatUtils.format(end, "HH:mm:ss");
                        int sumMoney = 0;
                        for (OrderInfo orderInfo : iterable) {
                            sumMoney += orderInfo.getMoney();
                        }
                        collector.collect("开始时间：" + startStr + "结束时间：" + endStr + "，用户id=" + integer + ",订单总额：" + sumMoney);
                    }
                });

        resultStream.print();
        resultStream.getSideOutput(sideOutput).print("严重迟到数据");
        env.execute();
/*
        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
       // env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        //2. source-加载数据
        DataStreamSource<OrderInfo> dataStreamSource = env.addSource(new MySource());


        OutputTag<OrderInfo> outputTag = new OutputTag<OrderInfo>("side output"){};

        SingleOutputStreamOperator<String> resultStream = dataStreamSource
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(
                        new SerializableTimestampAssigner<OrderInfo>() {
                            // long 是时间戳吗？是秒值还是毫秒呢？年月日时分秒的的字段怎么办呢？
                            @Override
                            public long extractTimestamp(OrderInfo orderInfo, long recordTimestamp) {
                                // 这个方法的返回值是毫秒，所有的数据只要不是这个毫秒值，都需要转换为毫秒
                                return orderInfo.getTimeStamp();
                            }
                        }
                )).keyBy(orderInfo -> orderInfo.getUid())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(5)) // 允许再迟到5秒，不过这个数据是可以进入窗口，窗口为其继续保留5秒钟，超过5秒的数据，不再进入窗口，直接被丢弃掉
                .sideOutputLateData(outputTag)
                .apply(new WindowFunction<OrderInfo, String, Integer, TimeWindow>() {
                    @Override
                    public void apply(Integer integer, TimeWindow window, Iterable<OrderInfo> input, Collector<String> out) throws Exception {
                        long start = window.getStart();
                        long end = window.getEnd();
                        String startStr = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss");
                        String endStr = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss");
                        int sumMoney = 0;
                        for (OrderInfo orderInfo : input) {
                            sumMoney += orderInfo.getMoney();
                        }
                        out.collect("开始时间：" + startStr + "结束时间：" + endStr + "，用户id=" + integer + ",订单总额：" + sumMoney);
                    }
                });
        resultStream.print();
        resultStream.getSideOutput(outputTag).print("严重迟到的数据:");
        //4. sink-数据输出


        //5. execute-执行
        env.execute();*/

    }
}
