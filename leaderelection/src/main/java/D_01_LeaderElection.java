import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class D_01_LeaderElection implements Watcher {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final String ELECTION_NAMESPACE = "/election";
    private ZooKeeper zooKeeper;
    private String currentZnodeName;

    public static void main(String[] args) {
        D_01_LeaderElection demo01LeaderElection = new D_01_LeaderElection();
        try {
            System.out.println(demo01LeaderElection.getClass().getSimpleName());
            demo01LeaderElection.connectToZookeeper();
            demo01LeaderElection.volunteerForLeadership();
            demo01LeaderElection.electLeader();
            demo01LeaderElection.run();
            demo01LeaderElection.close();
            System.out.println("exiting application");
        }
        catch (IOException ex){
            ex.printStackTrace();
        }
        catch (KeeperException e){
            e.printStackTrace();
        }
        catch (InterruptedException e){
            e.printStackTrace();
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

    private void volunteerForLeadership() throws KeeperException, InterruptedException {
        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("znode name = " + znodeFullPath);
        this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE+"/", "");
    }

    private void electLeader() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
        Collections.sort(children);
        String smallestChild = children.get(0);

        if(smallestChild.equals(currentZnodeName)){
            System.out.println("I am the leader: " + currentZnodeName);
        }
        else {
            System.out.println("I'm not the leader; the leader is: " + smallestChild);
        }

    }
}
