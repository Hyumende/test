package data02;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.UUID;
/**
 * lombok注解
 *
 * @Getter/@Setter：自动生成访问器和修改器
 *
 * @ToString：生成toString()方法
 *
 * @EqualsAndHashCode：生成equals()和hashCode()
 *
 * @NoArgsConstructor/@AllArgsConstructor：生成无参和全参构造器
 *
 * @Data：组合了@Getter、@Setter、@ToString、@EqualsAndHashCode等
 *
 * @Builder：生成Builder模式构造器，便于链式创建对象
 * */
@Data //setter getter toString
@NoArgsConstructor
@AllArgsConstructor
class OrderInfo{
    private String orderId;
    private int uid;
    private int money;
    private long timeStamp;
    private String time;

}
//public class MyOrderSource implements SourceFunction<OrderInfo> {
//public class MyOrderSource implements ParallelSourceFunction<OrderInfo> {
public class MyOrderSource extends RichParallelSourceFunction<OrderInfo> {

    @Override
    public void open(Configuration parameters) throws Exception {
        //System.out.println("此处可以进行数据库连接或者其别的提前需要准备的操作");
    }

    @Override
    public void close() throws Exception {
      //  System.out.println("此处可以进行一些收尾工作");
    }

    boolean isRun = true;
    Random random = new Random();
    @Override
    public void run(SourceContext<OrderInfo> sct) throws Exception {
        //传入参数
        while(isRun){
         OrderInfo orderInfo = new OrderInfo();
         orderInfo.setOrderId(UUID.randomUUID().toString().replace("-"," "));
         orderInfo.setUid(random.nextInt(10));
         orderInfo.setMoney(random.nextInt(1000));
         orderInfo.setTimeStamp(System.currentTimeMillis());
         sct.collect(orderInfo);
         Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRun = false;
    }
}
