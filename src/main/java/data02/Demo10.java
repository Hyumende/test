package data02;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.FileRecordFormat;
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
 * @create:2025-12-21 01:25:00
 **/
public class Demo10 {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(),new Path("datas/a.log")).build();
        DataStreamSource<String> dataStreamSource = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "filter算子");
        //2. source-加载数据
        //3. transformation-数据处理转换
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
        }).filter(new FilterFunction<LogBean>() {
            @Override
            public boolean filter(LogBean value) throws Exception {
               if (value.getIp().equals("83.149.9.216")){
                   return true;
               }
               return false;
            }
        }).print();

        //4. sink-数据输出

        //5. execute-执行
        env.execute();
    }
}