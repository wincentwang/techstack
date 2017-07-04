package wang.wincent.techstack.zookeeper.zk;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

public class SimpleZkClient {

    private static final String connectString = "192.168.244.100:2181,192.168.244.101:2181,192.168.244.102:2181";
    private static final int sessionTimeout = 2000;
    ZooKeeper zkClient = null;

    @Before
    public void init() throws Exception {
        System.out.println("init");
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("-----Connected");

                System.out.println(event.getType() + "---" + event.getPath());
                try {
                   zkClient.getChildren("/", true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        zkClient.getChildren("/", true);
        List<String> children = zkClient.getChildren("/", true);
        for (String child : children) {
            System.out.println(child);
        }
    }

    @Test
    public void testConnect() throws InterruptedException {
        System.out.println("connect...");
        Thread.sleep(Long.MAX_VALUE);
    }



    /**
     * 数据的增删改查
     *
     * @throws InterruptedException
     * @throws KeeperException
     */

    // 创建数据节点到zk中
    public void testCreate() throws KeeperException, InterruptedException {
        // 参数1：要创建的节点的路径 参数2：节点大数据 参数3：节点的权限 参数4：节点的类型
        String nodeCreated = zkClient.create("/eclipse", "hellozk".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        //上传的数据可以是任何类型，但都要转成byte[]
    }

    //判断znode是否存在
    @Test
    public void testExist() throws Exception {
        Stat stat = zkClient.exists("/eclipse", false);
        System.out.println(stat == null ? "not exist" : "exist");
    }

    // 获取子节点
    @Test
    public void getChildren() throws Exception {
        Thread.sleep(10000);
        System.out.println("connecting....");
        List<String> children = zkClient.getChildren("/", true);
        for (String child : children) {
            System.out.println(child);
        }


    }

    //获取znode的数据
    @Test
    public void getData() throws Exception {
        byte[] data = zkClient.getData("/eclipse", false, null);
        System.out.println(new String(data));
    }

    //删除znode
    @Test
    public void deleteZnode() throws Exception {
        //参数2：指定要删除的版本，-1表示删除所有版本
        zkClient.delete("/eclipse", -1);
    }

    //删除znode
    @Test
    public void setData() throws Exception {
        zkClient.setData("/app1", "imissyou angelababy".getBytes(), -1);
        byte[] data = zkClient.getData("/app1", false, null);
        System.out.println(new String(data));

    }


}
