import org.junit.Test;
import redis.clients.jedis.Jedis;

public class Redis_Demo {
  @Test
    public void testConnect(){
      Jedis jedis = new Jedis("192.168.154.100",6379);
      jedis.close();
      jedis.set("sbuject","大数据");


  }

}
