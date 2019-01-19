package Zoozy;

import org.apache.commons.io.IOUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.ZooKeeper;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class Moozy {


    private static final String string = "/hiveserver2/serverUri=ip-10-11-21-187.alationdata.com:10001;version=1.2.1000.2.6.5.0-292;sequence=0000000005";

    public static void main() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient("ip-10-11-21-187.alationdata.com:2181", retryPolicy);
        client.start();
        dunno(client.getZookeeperClient().getZooKeeper());
//        wheeee(client.getZookeeperClient().getZooKeeper(), "/", 0);
    }

    public static void wheeee(ZooKeeper zookeeper, String path, int tabs) throws Exception {
        for (int i = 0; i < tabs; i++) {
            System.out.print("\t");
        }
        System.out.println(path);
//        System.out.println(Paths.get(path).getFileName());
        for (String child : zookeeper.getChildren(path, false)) {
            try {
                wheeee(zookeeper, Paths.get(path, child).toString(), tabs + 1);
            } catch (Exception e) {
             System.out.println(e.getMessage());
            }
        }
    }

    public static void dunno(ZooKeeper zooKeeper) throws Exception {
        byte[] data = zooKeeper.getData(string, false,null);
        String good = IOUtils.toString(data);
        System.out.println("Asdasd");
    }
}
