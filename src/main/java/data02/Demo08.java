package data02;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @author: xusong
 * @create:2025-12-20 20:09:12
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
class LogBean{
    String ip;      // 访问ip
    int userId;     // 用户id
    long timestamp; // 访问时间戳
    String method;  // 访问方法
    String path;    // 访问路径
}
public class Demo08 {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2. source-加载数据
        FileSource<String> file = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),new Path("datas/a.log")
        ).build();
        DataStreamSource<String> dataStreamSource = env.fromSource(file, WatermarkStrategy.noWatermarks(), "map算子");

        //3. transformation-数据处理转化，将a.log中的数据进行切割，将每个属性都封装为logbean对象然后打印输出
        dataStreamSource.map(new MapFunction<String, LogBean>() {
            @Override
            public LogBean map(String line) throws Exception {
                String[] word = line.split(" ");
                LogBean logBean = new LogBean();
                logBean.setIp(word[0]);
                logBean.setPath(word[4]);
                logBean.setMethod(word[3]);
                logBean.setUserId(Integer.parseInt(word[1]));
                String timeStr = word[2];
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                Date date = simpleDateFormat.parse(timeStr);
                logBean.setTimestamp(date.getTime());
                return logBean;
            }
        }).print();
        //4. sink-数据输出

        //5. execute-执行
        env.execute();
    }
}