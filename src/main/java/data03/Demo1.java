package data03;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
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
public class Demo1 {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Student> dataStreamSource = env.fromElements(
                new Student(1, "张三", 18),
                new Student(2, "李四", 19),
                new Student(3, "王五", 20)

        );
        //2. source-加载数据
        //3. transformation-数据处理转换
        //4. sink-数据输出
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder jdbcConnectionOptionsBuilder = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder();
        JdbcConnectionOptions jdbcConnectionOptions = jdbcConnectionOptionsBuilder
                .withUrl("jdbc:mysql://root:3308/py_sql")
                .withUsername("root")
                .withPassword("123456")
                .withDriverName("com.mysql.cj.jdbc.Driver").build();
        SinkFunction<Student> sink = JdbcSink.sink(
                "insert into stu(id,name,age)values(null,?,?)", new JdbcStatementBuilder<Student>() {
                    @Override
                    public void accept(PreparedStatement stat, Student student) throws SQLException {
                        stat.setString(1, student.getName());
                        stat.setInt(2, student.getAge());
                    }
                }, jdbcConnectionOptions
        );
        dataStreamSource.addSink(sink);

        //5. execute-执行
        env.execute();
    }
}