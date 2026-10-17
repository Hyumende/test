package data02_02;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class PrintSinkDemo extends RichSinkFunction<String> {
    @Override
    public void invoke(String value, Context context) throws Exception {
         int partition =  getRuntimeContext().getIndexOfThisSubtask()+1;
        System.out.println(partition+">"+value);
    }
}
