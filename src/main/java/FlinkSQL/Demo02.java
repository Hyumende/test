package FlinkSQL;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @author: xuesong
 * @create:2025-12-31 21:01:34
 **/
public class Demo02 {
    public static void main(String[] args) throws Exception {
        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 2L, 2),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3),
                new WaterSensor("s3", 4L, 4)
        );

        tableEnvironment.createTemporaryView(
                "source",sensorDS
        );
        Table table = tableEnvironment.sqlQuery("select * from source where ts>2");

        DataStream<WaterSensor> rowDataStream = tableEnvironment.toDataStream(table,WaterSensor.class);

        rowDataStream.print();


        Table table1 = tableEnvironment.sqlQuery("select id,sum(vc) as sumVc from source group by id");

        DataStream<Row> rowDataStream1 = tableEnvironment.toChangelogStream(table1);
        rowDataStream1.print();

        //统计每个人员的总的vc的数量

        //2. source-加载数据
        //3. transformation-数据处理转换
        //4. sink-数据输出

        //5. execute-执行
        env.execute();
    }
}
