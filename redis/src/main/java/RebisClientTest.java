import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;

import java.util.Set;

/**
 * @author 王继昌
 * @create 2020-10-16 14:28
 */
public class RebisClientTest {
    public Jedis jedis;
    @Before
    public void  init(){
        //主机地址
        String host = "hadoop102";
        //通过工具类获取端口号
        int defaultPort = Protocol.DEFAULT_PORT;
        //获取Jedis对象
        jedis = new Jedis(host, 6000);
    }
    @After
    public void close(){
        if (jedis != null) {
            jedis.close();
        }
    }

    @Test
    public void testjedis(){
        String ping = jedis.ping();
        System.out.println(ping);
    }

    @Test
    public void testkey() {
        //查询
        Set<String> keys = jedis.keys("*");
        for (String key : keys) {
            System.out.println(key);
        }
        //类型
        String k1 = jedis.type("k1");
        System.out.println(k1);
        //删除
        Long k3 = jedis.del("k3");
        System.out.println(k3);
        //是否存在
        Boolean k31 = jedis.exists("k3");
        System.out.println(k31);
        //将值指定成临时文件
        Long k2 = jedis.expire("k2", 60);
        System.out.println(k2);
        //将看剩余时间
        Long k21 = jedis.ttl("k2");
        System.out.println(k21);
        //将临时创建成永久
        Long k22 = jedis.persist("k2");
        System.out.println(k22);
        //将看剩余时间
        Long k23 = jedis.ttl("k2");
        System.out.println(k23);
    }

    @Test
    public void  testString(){
        //创建数据
        String h1 = jedis.set("h1", "12");
        System.out.println(h1);
        //查看数据
        String h11 = jedis.get("h1");
        System.out.println(h11);
        //追加数据
        Long h12 = jedis.append("h1", "23");
        System.out.println(h12);
    }
    @Test
    public void  testList(){

    }
    @Test
    public void  testset(){

    }
    @Test
    public void  testzset(){

    }

    @Test
    public void testjedisPool(){
        String host = "hadoop102";
        JedisPool jedisPool = new JedisPool(host, 6000);
        Jedis resource = jedisPool.getResource();
        String ping = resource.ping();
        System.out.println(ping);
        resource.close();
    }
}
