package data02;

import lombok.Data;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.swing.text.DateFormatter;
import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class Demo07 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);



        DataStreamSource<OrderInfo> dataStreamSource = env.addSource(new MyOrderSource());

        SingleOutputStreamOperator<OrderInfo> orderInfoSingleOutputStreamOperator = dataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
            @Override
            public long extractTimestamp(OrderInfo orderInfo, long l) {
                return orderInfo.getTimeStamp();
            }
        }));

        orderInfoSingleOutputStreamOperator.keyBy( order -> order.getUid())
                        .window(TumblingProcessingTimeWindows.of(Time.seconds(5))).process(new ProcessWindowFunction<OrderInfo, Object, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, ProcessWindowFunction<OrderInfo, Object, Integer, TimeWindow>.Context context, Iterable<OrderInfo> iterable, Collector<Object> collector) throws Exception {
                        int sumMoney = 0;
                        for (OrderInfo orderInfo : iterable) {

                           // out.collect("开始时间："+startStr+"结束时间："+endStr+"，用户id="+integer+",订单总额："+sumMoney);
                            String startTime = DateFormatUtils.format(context.window().getStart(),"yyyy-MM-dd HH:mm:ss");
                            String endTime = DateFormatUtils.format(context.window().getEnd(),"yyyy-MM-dd HH:mm:ss");
                             sumMoney+=orderInfo.getMoney();
                            collector.collect("开始时间"+startTime+"结束时间"+endTime+",用户id="+integer+",订单总额"+sumMoney);
                        }
                    }

                    @Override
                    public void clear(ProcessWindowFunction<OrderInfo, Object, Integer, TimeWindow>.Context context) throws Exception {
                        super.clear(context);
                    }
                }).print();

        env.execute();

    }
}
