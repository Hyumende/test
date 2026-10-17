package data03;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @author: xusong
 * @create:2025-12-21 19:34:27
 **/
public class Demo2 {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Student> dataStreamSource = env.fromElements(
                new Student(1, "赵六", 18),
                new Student(2, "田七", 19),
                new Student(3, "王八", 20)

        );
        //2. source-加载数据
        //3. transformation-数据处理转换
        //4. sink-数据输出
        dataStreamSource.addSink(new MyJdbcSink());

        //5. execute-执行
        env.execute();
    }
}