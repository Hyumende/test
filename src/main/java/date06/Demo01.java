package date06;

import lombok.SneakyThrows;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @author: xuesong
 * @create:2026-03-01 17:56:46
 **/
public class Demo01 {
    public static void main(String[] args) throws Exception {
        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStreamSource<LoginEvent> ds1 = env.fromElements(
                new LoginEvent("1", "fail", "2023-07-18 10:10:20"),
                new LoginEvent("1", "success", "2023-07-18 10:10:21"),
                new LoginEvent("1", "fail", "2023-07-18 10:10:22"),
                new LoginEvent("1", "fail", "2023-07-18 10:13:25"),
                new LoginEvent("1", "fail", "2023-07-18 10:18:30"),
                new LoginEvent("1", "fail", "2023-07-18 10:18:30"),
                new LoginEvent("2", "fail", "2023-07-18 10:10:21"),
                new LoginEvent("2", "fail", "2023-07-18 10:10:22"),
                new LoginEvent("2", "success", "2023-07-18 10:10:23"),
                new LoginEvent("2", "fail", "2023-07-18 10:10:24")
        );

        // 此处需要添加水印
        SingleOutputStreamOperator<LoginEvent> ds2 = ds1.assignTimestampsAndWatermarks(
                WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                            @SneakyThrows
                            @Override
                            public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                                Date date = DateUtils.parseDate(element.getLonginTime(), "yyyy-MM-dd HH:mm:ss");
                                // 返回值是毫秒
                                return date.getTime();
                            }
                        })
        );

        //定义模式 三分钟之内连续失败两次

        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("first", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.getType().equals("fail");
                    }
                }).times(2).consecutive().within(Time.minutes(3));


        //将该模式，作用在某个流上
        PatternStream<LoginEvent> patternStream = CEP.pattern(ds2.keyBy(v -> v.getId()), pattern);

        //展示结果
        
        patternStream.select(new PatternSelectFunction<LoginEvent, Object>() {
            @Override
            public Object select(Map<String, List<LoginEvent>> map) throws Exception {

                //System.out.println(map);
                Set<String> keys = map.keySet();
                for (String key : keys) {
                    List<LoginEvent> loginEvents = map.get(key);
                    for (LoginEvent loginEvent : loginEvents) {
                        System.out.println(loginEvent);
                    }
                }
                return null;
            }
        });

        //3. transformation-数据处理转换
        //4. sink-数据输出

        //5. execute-执行
        env.execute();
    }
}
