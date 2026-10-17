package FlinkSQL;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @author: xuesong
 * @create:2026-01-02 21:15:27
 **/
public class Kakfa_processTime {
    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        tableEnv.executeSql("CREATE TABLE table1 (\n" +
                "  `username` string,\n" +
                "  `price` int,\n" +
                "  `event_time` as proctime()\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'flink',\n" +
                "  'properties.bootstrap.servers' = 'root:9092',\n" +
                "  'properties.group.id' = 'g1',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
        tableEnv.executeSql(
                "select \n" +
                        "   window_start,\n" +
                        "   window_end,\n" +
                        "   username,\n" +
                        "   count(1) zongNum,\n" +
                        "   sum(price) totalMoney \n" +
                        "   from table(HOP(TABLE table1, DESCRIPTOR(event_time), INTERVAL '60' second))\n" +
                        "group by window_start,window_end,username;"
        ).print();

        //2. 创建表对象
        //第一个表 对接kafka topic1
        //第二个表 对接kafka topic2
        //4. sink-数据输出

        //5. execute-执行

    }
}
