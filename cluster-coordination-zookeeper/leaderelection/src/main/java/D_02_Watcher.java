import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

public class D_02_Watcher implements Watcher{
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private ZooKeeper zooKeeper;
    private String TARGET_ZNODE = "/target_znode";

    public static void main(String[] args) {
        D_02_Watcher watcher = new D_02_Watcher();
        try {
            System.out.println(watcher.getClass().getSimpleName());
            watcher.connectToZookeeper();
            watcher.watchTargetZnode();
            watcher.run();
            watcher.close();
            System.out.println("exiting application");
        }
        catch (IOException ex){
            ex.printStackTrace();
        }
    }

    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    public void process(WatchedEvent event) {
        switch (event.getType()){
            case None: if(event.getState() == Watcher.Event.KeeperState.SyncConnected){
                System.out.println("successfully connected to zookeeper");
            }
            else {
                synchronized (zooKeeper){
                    System.out.println("disconnected from zookeeper");
                    zooKeeper.notifyAll();
                }
            }
            break;

            case NodeDeleted:
                System.out.println(TARGET_ZNODE + " node deleted"); break;
            case NodeCreated:
                System.out.println(TARGET_ZNODE + " node created"); break;
            case NodeDataChanged:
                System.out.println(TARGET_ZNODE + " node data changed"); break;
            case NodeChildrenChanged:
                System.out.println(TARGET_ZNODE + " node children changed"); break;
        }

        watchTargetZnode();

    }

    private void watchTargetZnode(){
        try {
            Stat stat = zooKeeper.exists(TARGET_ZNODE, this);
            if(stat == null){
                return;
            }
            byte[] data = zooKeeper.getData(TARGET_ZNODE, this, stat);
            List<String> children = zooKeeper.getChildren(TARGET_ZNODE, this);

            System.out.println("data: " + new String(data) + " children: " + children);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public void run(){
        synchronized (zooKeeper){
            try {
                zooKeeper.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void close(){
        try {
            this.zooKeeper.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}
