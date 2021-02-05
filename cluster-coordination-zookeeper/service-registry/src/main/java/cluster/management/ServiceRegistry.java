package cluster.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ServiceRegistry implements Watcher {
    private static final String REGISTRTY_ZNODE = "/service_registry";
    private final ZooKeeper zooKeeper;
    private String currentZnode;
    private List<String> allServiceAddresses = null;

    public ServiceRegistry(ZooKeeper zooKeeper){
        this.zooKeeper = zooKeeper;
        createServiceRegistryZnode();
    }

    private void createServiceRegistryZnode(){
        // race condition is possible here:
        // two nodes might try to create znode at the same time.
        // this is taken care of zookeeper internally. Only one call to create is allowed to succeed.
        try {
            if(zooKeeper.exists(REGISTRTY_ZNODE, false) == null){
                zooKeeper.create(REGISTRTY_ZNODE, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void registerToCluster(String metadata) throws KeeperException, InterruptedException {
        this.currentZnode = zooKeeper.create(REGISTRTY_ZNODE + "/n_",
                metadata.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("registered to cluster: " + currentZnode.replace(REGISTRTY_ZNODE+"/", ""));

    }

    private synchronized void updateAddresses() throws KeeperException, InterruptedException {
        List<String> workerZnodes = zooKeeper.getChildren(REGISTRTY_ZNODE, this);
        List<String> addresses = new ArrayList<>(workerZnodes.size());
        for(String workerZnode : workerZnodes){
            String workerZnodeFullPath = REGISTRTY_ZNODE + "/" + workerZnode;
            Stat stat = zooKeeper.exists(workerZnodeFullPath, this);
            if(stat == null){
                continue;
            }
            byte[] addressBytes = zooKeeper.getData(workerZnodeFullPath, false, stat);
            String address = addressBytes.toString();
            addresses.add(address);
        }

        this.allServiceAddresses = Collections.unmodifiableList(addresses);
        System.out.println("the cluster addresses are: " + allServiceAddresses);
    }

    public void registerForUpdates(){
        try {
            updateAddresses();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private List<String> getAllServiceAddresses() throws KeeperException, InterruptedException {
        if(allServiceAddresses == null){
            updateAddresses();
        }
        return allServiceAddresses;
    }

    public void unregisterFromCluster()  {
        try {
            if(currentZnode != null && zooKeeper.exists(currentZnode, false) != null){
                zooKeeper.delete(currentZnode, -1);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        try {
            updateAddresses();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
