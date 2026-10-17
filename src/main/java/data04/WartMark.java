package data04;

import data02.MyOrderSource;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @author: xuesong
 * @create:2025-12-26 00:32:54
 **/

class MyorderInfo implements SourceFunction<OrderInfo>{

    @Override
    public void run(SourceContext<OrderInfo> sourceContext) throws Exception {
        Random random = new Random();
        while (true){
            OrderInfo orderInfo = new OrderInfo();
            orderInfo.setOrderId(UUID.randomUUID().toString());
            orderInfo.setUid(random.nextInt(3));
            orderInfo.setMoney(random.nextInt(100));
            orderInfo.setTimeStamp(System.currentTimeMillis());
            sourceContext.collect(orderInfo);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {

    }
}
public class WartMark {
    public static void main(String[] args) throws Exception {
        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
        DataStreamSource<OrderInfo> dataStreamSource = env.addSource(new MyorderInfo());
        //2. source-加载数据
        //3. transformation-数据处理转换
        //4. sink-数据输出
        dataStreamSource
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo orderInfo, long l) {
                        //这个方法返回值是行号秒，所有数据只要不是这个毫秒值，都需要转为毫秒
                        return orderInfo.getTimeStamp();
                    }
                    }))
                .keyBy(orderInfo -> orderInfo.getUid())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum("money").print();
        //5. execute-执行
        env.execute();
    }
}
