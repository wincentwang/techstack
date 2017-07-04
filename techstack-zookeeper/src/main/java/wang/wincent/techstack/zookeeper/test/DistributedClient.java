package wang.wincent.techstack.zookeeper.test;


import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by user on 7/3/2017.
 */
public class DistributedClient{

        // 超时时间
        private static final int SESSION_TIMEOUT = 5000;
        // zookeeper server列表
        private String hosts = "hadoop1:2181,hadoop2:2181,hadoop3:2181";
        private String groupNode = "locks";
        private String subNode = "sub";

        private ZooKeeper zk;
        // 当前client创建的子节点
        private String thisPath;
        // 当前client等待的子节点
        private String waitPath;

        private CountDownLatch latch = new CountDownLatch(1);

        /**
         * 连接zookeeper
         */
        public void connectZookeeper() throws Exception {
            zk = new ZooKeeper(hosts, SESSION_TIMEOUT, new Watcher() {
                public void process(WatchedEvent event) {
                    System.out.println(123);
                    try {
                        // 连接建立时, 打开latch, 唤醒wait在该latch上的线程
                        if (event.getState() == Event.KeeperState.SyncConnected) {
                            latch.countDown();
                        }
                        // 发生了waitPath的删除事件
                        if (event.getType() == Event.EventType.NodeDeleted && event.getPath().equals(waitPath)) {
                            doSomething();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
            // 等待连接建立
            latch.await();
            // 创建子节点
            thisPath = zk.create("/" + groupNode + "/" + subNode, null, ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
            // wait一小会, 让结果更清晰一些
            Thread.sleep(10);
            // 注意, 没有必要监听"/locks"的子节点的变化情况
            List<String> childrenNodes = zk.getChildren("/" + groupNode, false);
            // 列表中只有一个子节点, 那肯定就是thisPath, 说明client获得锁
            if (childrenNodes.size() == 1) {
                doSomething();
            } else {
                String thisNode = thisPath.substring(("/" + groupNode + "/").length());
                Collections.sort(childrenNodes);
                int index = childrenNodes.indexOf(thisNode);
                if (index == -1) {
                    // never happened
                } else if (index == 0) {
                    // inddx == 0, 说明thisNode在列表中最小, 当前client获得锁
                    doSomething();
                } else {
                    // 获得排名比thisPath前1位的节点
                    this.waitPath = "/" + groupNode + "/" + childrenNodes.get(index - 1);
                    // 在waitPath上注册监听器, 当waitPath被删除时, zookeeper会回调监听器的process方法
                    zk.getData(waitPath, true, new Stat());
                }
            }
        }

        private void doSomething() throws Exception {
            try {
                System.out.println("gain lock: " + thisPath);
                Thread.sleep(2000);
                // do something
            } finally {
                System.out.println("finished: " + thisPath);
                // 将thisPath删除, 监听thisPath的client将获得通知
                // 相当于释放锁
                zk.delete(this.thisPath, -1);
            }
        }

        public static void main(String[] args) throws Exception {
//            for (int i = 0; i < 1; i++) {
//                new Thread() {
//                    public void run() {
//                        try {
//                            DistributedClient dl = new DistributedClient();
//                            dl.connectZookeeper();
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//                    }
//                }.start();
//            }
//
//            Thread.sleep(Long.MAX_VALUE);
            DistributedClient dl = new DistributedClient();
            dl.connectZookeeper();
            Thread.sleep(Long.MAX_VALUE);
        }
}
