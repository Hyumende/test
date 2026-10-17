package Flink_Doris;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.net.ntp.TimeStamp;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderCdcData {
    private long id;
    private BigDecimal total_amount;
    private BigDecimal pay_amount;
    private int status;
    private int confirm_status;
    private TimeStamp create_time;
    private TimeStamp confirm_time;
    private TimeStamp payment_time;
    private TimeStamp update_time;

}
