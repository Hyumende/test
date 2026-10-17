package FlinkSQL;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @author: xuesong
 * @create:2026-01-02 21:15:27
 **/
public class Kakfa_connect {
    public static void main(String[] args) throws Exception {
        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE table1 (\n" +
                "  `user_id` int,\n" +
                "  `page_id` int,\n" +
                "  `status` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'first',\n" +
                "  'properties.bootstrap.servers' = 'root:9092',\n" +
                "  'properties.group.id' = 'g1',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
        tableEnv.executeSql(
                "CREATE TABLE table2 (\n" +
                        "  `user_id` int,\n" +
                        "  `page_id` int,\n" +
                        "  `status` STRING\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'flink',\n" +
                        "  'properties.bootstrap.servers' = 'root:9092',\n" +
                        "  'properties.group.id' = 'g1',\n" +
                        "  'scan.startup.mode' = 'latest-offset',\n" +
                        "  'format' = 'json'\n" +
                        ")"
        );
        tableEnv.executeSql(
                "insert into table2 select * from table1 where status='success'"
        );
        //2. 创建表对象
        //第一个表 对接kafka topic1
        //第二个表 对接kafka topic2
        //4. sink-数据输出

        //5. execute-执行

    }
}
