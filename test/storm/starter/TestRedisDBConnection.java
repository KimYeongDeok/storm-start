package storm.starter;

import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class TestRedisDBConnection {
    @Test
    public void test(){
        Jedis jedis = new Jedis("61.43.139.65");
        jedis.set("word", "1");
    }

}
