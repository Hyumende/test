package data02;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.UUID;

public class MySource implements SourceFunction<OrderInfo> {
    private boolean flag = true;
    @Override
    public void cancel() {
        flag = false;
    }
    @Override
    public void run(SourceContext<OrderInfo> sourceContext) throws Exception {
        // 源源不断的产生数据
        Random random = new Random();
        while(flag) {
            OrderInfo orderInfo = new OrderInfo();
            orderInfo.setOrderId(UUID.randomUUID().toString());
            orderInfo.setUid(random.nextInt(3));
            orderInfo.setMoney(random.nextInt(101));

            // 模拟订单产生的真实时间，减去100秒，模拟迟到的数据
            long orderTime = System.currentTimeMillis() - 1000 * random.nextInt(100);
            orderInfo.setTimeStamp(orderTime);
            orderInfo.setTime(DateFormatUtils.format(orderTime,"HH:mm:ss"));
            sourceContext.collect(orderInfo);
            Thread.sleep(1000);// 间隔1s
        }
    }
}
