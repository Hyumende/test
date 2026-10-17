package STUDY;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.text.ParseException;
import java.util.Date;
//使用KeyedState中的ValueState获取数据中的最大值
public class test {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

         tenv.executeSql("CREATE TABLE table1 (\n" +
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

         tenv.executeSql("CREATE TABLE table2 (\n" +
                 "  `user_id` int,\n" +
                 "  `page_id` int,\n" +
                 "  `status` STRING\n" +
                 ") WITH (\n" +
                 "    'connector' = 'jdbc',\n" +
                 "    'url' = 'jdbc:mysql://root:3308/py_sql?useUnicode=true&characterEncoding=utf8',\n" +
                 "    'table-name' = 't_success', \n" +
                 "    'username' = 'root',\n" +
                 "    'password' = '123456'\n" +
                 ")");

        tenv.executeSql("insert into table2 select * from table1 where status <> 'success'");




    }
}
