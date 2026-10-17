package data04;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @author: xuesong
 * @create:2025-12-26 00:32:54
 **/

class MyorderInfo1 implements SourceFunction<OrderInfo>{

    @Override
    public void run(SourceContext<OrderInfo> sourceContext) throws Exception {
        Random random = new Random();
        while (true){
            OrderInfo orderInfo = new OrderInfo();
            orderInfo.setOrderId(UUID.randomUUID().toString());
            orderInfo.setUid(random.nextInt(3));
            orderInfo.setMoney(random.nextInt(100));
            orderInfo.setTimeStamp(System.currentTimeMillis()-1000*random.nextInt(10));
            sourceContext.collect(orderInfo);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {

    }
}

class all_laterWartMark {
    public static void main(String[] args) throws Exception {
        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
        DataStreamSource<OrderInfo> dataStreamSource = env.addSource(new MyorderInfo());
        OutputTag<OrderInfo> outputTag = new OutputTag<OrderInfo>("side output"){};
        //2. source-加载数据
        //3. transformation-数据处理转换
        //4. sink-数据输出
        SingleOutputStreamOperator<String> streamOperator = dataStreamSource
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo orderInfo, long l) {
                        //这个方法返回值是毫秒值，所有数据只要不是这个毫秒值，都需要转为毫秒
                        return orderInfo.getTimeStamp();
                    }
                }))
                .keyBy(orderInfo -> orderInfo.getUid())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(5))
                .sideOutputLateData(outputTag)
                .apply(new WindowFunction<OrderInfo, String, Integer, TimeWindow>() {
                    @Override
                    public void apply(Integer integer, TimeWindow timeWindow, Iterable<OrderInfo> iterable, Collector<String> collector) throws Exception {
                        int sum = 0;
                        long start = timeWindow.getStart();
                        long end = timeWindow.getEnd();
                        String startTime = DateFormatUtils.format(start, "mm-dd HH:mm:ss");
                        String endTime = DateFormatUtils.format(end, "mm-dd HH:mm:ss");
                        for (OrderInfo orderInfo : iterable) {
                            sum+=orderInfo.getMoney();
                            collector.collect("开始时间："+startTime+","+"结束时间："+endTime+" "+"订单id="+orderInfo.getUid()+","+"订单金额money="+orderInfo.getMoney());
                        }


                    }
                });
        streamOperator.print("正常 或者迟到数据");
        streamOperator.getSideOutput(outputTag).print("严重迟到数据");
        //5. execute-执行
        env.execute();
    }
}
