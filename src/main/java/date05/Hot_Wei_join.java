package date05;

import com.google.common.cache.*;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @author: xuesong
 * @create:2025-12-28 12:50:21
 **/
public class Hot_Wei_join {
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
            // 定义一个Cache
            LoadingCache<String, String> cache;
            @Override
            public void open(Configuration parameters) throws Exception {
                String url = "jdbc:mysql://root:3308/py_sql?useSSL=false&serverTimezone=UTC";
                String username = "root";  // 通常是 root
                String password = "123456";
                connection = DriverManager.getConnection(url, username, password);
                //执行此句
                statement = connection.prepareStatement("select * from py_sql.city where city_id=?");

                cache = CacheBuilder.newBuilder()
                        //最多缓存个数，超过了就根据最近最少使用算法来移除缓存 LRU
                        .maximumSize(1000)
                        //在更新后的指定时间后就回收
                        // 不会自动调用，而是当过期后，又用到了过期的key值数据才会触发的。
                        .expireAfterWrite(100, TimeUnit.SECONDS)
                        //指定移除通知,打印了过期移除了什么值
                        .removalListener(new RemovalListener<String, String>() {
                            @Override
                            public void onRemoval(RemovalNotification<String, String> removalNotification) {
                                System.out.println(removalNotification.getKey() + "被移除了，值为：" + removalNotification.getValue());
                            }
                        })
                        .build(//指定加载缓存的逻辑
                                new CacheLoader<String, String>() {
                                    // 假如缓存中没有数据，会触发该方法的执行，并将结果自动保存到缓存中
                                    @Override
                                    public String load(String cityId) throws Exception {
                                        System.out.println("进入数据库查询啦。。。。。。。");
                                        statement.setString(1,cityId);
                                        ResultSet resultSet = statement.executeQuery();
                                        String cityName = null;
                                        if(resultSet.next()){
                                            System.out.println("进入到了if中.....");
                                            cityName = resultSet.getString("city_name");
                                        }
                                        return cityName;
                                    }
                                });



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
                String cityId = split[1];
                String cityName = cache.get(cityId);
                return Tuple3.of(split[0],split[1],cityName);

            }
        }).print();
        
        //2. source-加载数据
        //3. transformation-数据处理转换
        //4. sink-数据输出

        //5. execute-执行
        env.execute();
    }
}
