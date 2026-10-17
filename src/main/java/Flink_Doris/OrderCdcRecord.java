package Flink_Doris;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderCdcRecord {

    OrderCdcData before;
    OrderCdcData After;
    String op;

}
