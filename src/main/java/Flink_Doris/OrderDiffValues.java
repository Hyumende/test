package Flink_Doris;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.net.ntp.TimeStamp;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Setter
public class OrderDiffValues {

    private TimeStamp OutTime = null;

    private int totalCount = 0;
    private BigDecimal  TotalOriginAmount = BigDecimal.ZERO;
    private BigDecimal  totalRealAmount = BigDecimal.ZERO;

    private int toPayTotalCount = 0;
    private BigDecimal   toPayAmount = BigDecimal.ZERO;



}
