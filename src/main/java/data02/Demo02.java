package data02;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(), new Path("datas/wc.txt")
        ).build();
/*        DataStreamSource<String> dataStreamSource = env.readTextFile("datas/wc.txt");
        dataStreamSource.print();*/
        DataStreamSource<String> dataStreamSource = env.readFile(new TextInputFormat(new Path("datas/wc.txt")), "datas/wc.txt");
        dataStreamSource.print();
        env.execute();

    }
}
