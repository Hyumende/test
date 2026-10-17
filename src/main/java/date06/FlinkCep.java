package date06;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.text.ParseException;
import java.time.Duration;
import java.util.*;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @author: xuesong
 * @create:2026-03-01 19:30:20
 **/
public class FlinkCep {
    public static void main(String[] args) throws Exception {
        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","root:9092");
        properties.setProperty("group.id","g1");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer("test",new SimpleStringSchema(), properties);
        DataStreamSource<String> ds1 = env.addSource(consumer);
        // 解析字符串
        SingleOutputStreamOperator<LoginEvent> ds2 = ds1.map(new MapFunction<String, LoginEvent>() {
            @Override
            public LoginEvent map(String s) throws Exception {
                return JSON.parseObject(s,LoginEvent.class);
            }
        });
        SingleOutputStreamOperator<LoginEvent> ds3 = ds2.assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                    @Override
                    public long extractTimestamp(LoginEvent loginEvent, long l) {
                        Date date = null;
                        try {
                            date = DateUtils.parseDate(loginEvent.getLonginTime(), "yyyy-MM-dd HH:mm:ss");
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        // 返回值是毫秒
                        return date.getTime();
                    }
                })
        );

        //创建模式

        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("first", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.getType().equals("create");
                    }
                }).followedBy("second").where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return loginEvent.getType().equals("pay");
                    }
                }).within(Time.minutes(10));

        //包装流
        PatternStream<LoginEvent> patternStream = CEP.pattern(ds3.keyBy(v -> v.getId()), pattern);

        SingleOutputStreamOperator<String> select = patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> map) throws Exception {

                Collection<List<LoginEvent>> values = map.values();
                StringBuffer sb = new StringBuffer();
                for (List<LoginEvent> value : values) {
                    for (LoginEvent payEvent : value) {
                        sb.append(payEvent.getId()).append(payEvent.getType());
                    }
                }
                return null;
            }
        });
        select.print();

        //5. execute-执行
        env.execute();
    }
}
