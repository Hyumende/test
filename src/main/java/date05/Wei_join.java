package date05;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.protocol.types.Field;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @author: xuesong
 * @create:2025-12-28 12:50:21
 **/
public class Wei_join {
    public static void main(String[] args) throws Exception {
        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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



        dataStreamSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                String[] strings = s.split(",");
                if(strings.length>=2){
                    return true;
                }else return false;
            }
        }).map(new RichMapFunction<String,Tuple3<String,String,String>>() {
            //设置方法
            Connection connection = null;
            PreparedStatement statement = null;
            @Override
            public void open(Configuration parameters) throws Exception {
                String url = "jdbc:mysql://root:3308/py_sql?useSSL=false&serverTimezone=UTC";
                String username = "root";  // 通常是 root
                String password = "123456";
                connection = DriverManager.getConnection(url, username, password);
                //执行此句
                statement = connection.prepareStatement("select * from py_sql.city where city_id=?");
            }

            @Override
            public void close() throws Exception {
                statement.close();
                connection.close();
            }

            @Override
            public Tuple3<String, String, String> map(String s) throws Exception {
                String[] split = s.split(",");
                //赋予？值
                statement.setString(1,split[1]);
                ResultSet resultSet = statement.executeQuery();
                if(resultSet.next()){
                    return Tuple3.of(split[0],split[1],resultSet.getString("city_name"));
                }
                else{ return Tuple3.of(split[0], "NOT_FOUND", split[1]);
                }
            }
        }).print();
        
        //2. source-加载数据
        //3. transformation-数据处理转换
        //4. sink-数据输出

        //5. execute-执行
        env.execute();
    }
}
