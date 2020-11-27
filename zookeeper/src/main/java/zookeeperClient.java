
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author 王继昌
 * @create 2020-09-12 10:32
 */
public class zookeeperClient {
    ZooKeeper zooKeeper;
    @Before
    public void open() throws IOException {
        zooKeeper = new ZooKeeper("hadoop102:2181,hadoop103:2181,hadoop104:2181", 2000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("zookeeper-启动");
            }
        });
    }


    @After
    public void close() throws InterruptedException {
        zooKeeper.close();
    }


    @Test
    public void create() throws KeeperException, InterruptedException {
//        String s = zooKeeper.create("/wang", "wangjichang".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        String s = zooKeeper.create("/atguigu", "jinlian".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(s);
    }

    @Test
    public void ls() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren("/", true);
        for (String child : children) {
            System.out.println(child);
        }
    }

    @Test
    public void set() throws KeeperException, InterruptedException {
        Stat stat = new Stat();
        List<String> children = zooKeeper.getChildren("/atguigu", true, stat);
        System.out.println(stat.toString());
        zooKeeper.setData("/atguigu","wangjichang".getBytes(),stat.getVersion());
    }

    @Test
    public void getValue() throws KeeperException, InterruptedException {
        Stat stat = new Stat();
        byte[] data = zooKeeper.getData("/atguigu", true, stat);
        System.out.println(data.toString());
    }

    // 判断znode是否存在
    @Test
    public void exist() throws Exception {

        Stat stat = zooKeeper.exists("/atguigu", false);

        System.out.println(stat == null ? "not exist" : "exist");
    }

    @Test
    public void listence(){
        Stat stat = new Stat();
        Watcher getprocess = new Watcher() {

            @Override
            public void process(WatchedEvent event) {
            }
        };
    }

    @Test
    public void getChildren(){
        try {
            List<String> children = zooKeeper.getChildren("/atguigu", new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                     getChildren();
                }
            });
            System.out.println("======================");
            children.forEach(System.out::println);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }


}
