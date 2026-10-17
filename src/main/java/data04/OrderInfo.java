package data04;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data  // set get toString
@AllArgsConstructor
@NoArgsConstructor
public class OrderInfo {
        private String orderId;
        private int uid;
        private int money;
        private long timeStamp;
}
