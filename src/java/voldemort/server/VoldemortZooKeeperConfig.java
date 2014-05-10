package voldemort.server;


import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import voldemort.VoldemortException;
import voldemort.headmaster.ActiveNodeZKListener;
import voldemort.store.configuration.ConfigurationStorageEngine;
import voldemort.tools.ZKDataListener;
import voldemort.utils.ConfigurationException;
import voldemort.utils.Props;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

public class VoldemortZooKeeperConfig extends VoldemortConfig implements ZKDataListener {
    private final static Logger logger = Logger.getLogger(VoldemortZooKeeperConfig.class);

    public final Object readyLock = new Object();

    private boolean isReady = false;

    private String zkURL;
    private String hostname;
    private String voldemortHome;
    private String voldemortConfigDir;

    private ZooKeeper zk;

    private ActiveNodeZKListener activeNodeZKListener;

    public VoldemortZooKeeperConfig(String voldemortHome, String voldemortConfigDir, String zkurl) throws ConfigurationException {
        zkURL = zkurl;
        this.voldemortHome = voldemortHome;
        this.voldemortConfigDir = voldemortConfigDir;
        try {
            this.hostname = InetAddress.getLocalHost().getCanonicalHostName().toString();
        } catch (UnknownHostException e) {
            throw new ConfigurationException("Unable to determine hostname of host", e);
        }
        activeNodeZKListener = new ActiveNodeZKListener(zkURL);
        activeNodeZKListener.addDataListener(this);

        zk = activeNodeZKListener.getZooKeeper();

        tryToReadConfig();
    }

    private synchronized void tryToReadConfig() {
        logger.info("Trying to (re?)read config...");

        // try to create a working config, if we exception, set up an exist watch and try to re-read config
        // after an event happens
        try {
            Props props = generateProps();
            setProps(props);

            setReady(true);
            synchronized (readyLock) {
                readyLock.notifyAll();
            }
        } catch (Exception e) {

            try {
                this.zk.exists("/config/nodes/" + this.hostname + "/server.properties", true);
            } catch (KeeperException e1) {
                logger.error(e1);
            } catch (InterruptedException e1) {
                logger.error(e1);
            }

        } finally {
            registerAliveness();
        }
    }

    private void registerAliveness() {
        Stat stat = null;

        String nodeid;
        if(isReady()) {
            nodeid = String.valueOf(getNodeId());
        } else {
            nodeid = VoldemortConfig.NEW_ACTIVE_NODE_STRING;
        }

        String path = "/active/" + this.hostname;

        try {
            stat = this.zk.exists(path, false);
            if (stat == null) {
                this.zk.create(path,
                        nodeid.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } else {
                zk.setData(path, nodeid.getBytes(), stat.getVersion());
            }
        } catch (KeeperException e) {
            logger.error(e);
        } catch (InterruptedException e) {
            logger.error(e);
        }
    }

    private synchronized void setReady(boolean b) {
        this.isReady = b;
    }

    private Props generateProps() throws IOException {
        Props props = loadConfigs(this.zk);
        props.put("voldemort.home", voldemortHome);

        props.put("metadata.directory", voldemortConfigDir);

        return props;
    }

    private Props loadConfigs(ZooKeeper zk) throws IOException {

        Props properties = null;

        String nodeproperties = getNodeConfigFromZooKeeper(zk);

        Properties propertiesData = new Properties();
        propertiesData.load(new StringReader(nodeproperties));
        properties = new Props(propertiesData);

        return properties;
    }

    public static VoldemortConfig loadFromZooKeeper(String voldemorthome, String voldeConfig, String zookeeperurl) {
        if(voldeConfig == null) {
            voldeConfig = voldemorthome + "/config";
        }
        VoldemortZooKeeperConfig voldemortConfig = new VoldemortZooKeeperConfig(voldemorthome, voldeConfig, zookeeperurl);

        synchronized (voldemortConfig.readyLock) {
            while(!voldemortConfig.isReady()) {
                try {
                    voldemortConfig.readyLock.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        return voldemortConfig;
    }

    private String getClusterConfigFromZooKeeper(ZooKeeper zk) {
        return getFileFromZooKeeper(zk, "/config/cluster.xml");
    }

    private String getFileFromZooKeeper(ZooKeeper zk, String path) throws VoldemortException {
        Stat stat = new Stat();
        String s = null;
        try {
            byte[] configdata = zk.getData(path, false, stat);
            s = new String(configdata);
        } catch (KeeperException e) {
            throw new VoldemortException(String.format("Error getting key from ZooKeeper: %s", path), e);
        } catch (InterruptedException e) {
            throw new ConfigurationException(String.format("Error getting key from ZooKeeper: %s", path), e);
        }
        return s;
    }


    @Override
    public void process(WatchedEvent event) {
        logger.info(String.format("Got event from ZooKeeper: %s", event));

        if (event.getState() == Event.KeeperState.Expired) {
            // session expired, we are dead and all is gone.
            zk = null;
        }

    }

    public String getNodeConfigFromZooKeeper(ZooKeeper zk) throws VoldemortException {
        return getFileFromZooKeeper(zk, "/config/nodes/"+this.hostname+"/server.properties");
    }

    private boolean isZooKeeperAlive() {
        if(zk == null || !zk.getState().isAlive()) {
            return false;
        }
        return true;
    }

    public ZooKeeper getZooKeeper() {
        if (isZooKeeperAlive()) {
            return this.zk;
        } else {
            logger.info("Asked for ZooKeeper object, but it is dead!");
        }
        return zk;
    }

    public void setWatcher(Watcher watcher) {
        activeNodeZKListener.addWatcher(watcher);

        logger.info("Registered " + watcher + " as watcher for ZooKeeper instance.");
    }

    public String getHostname() {
        return hostname;
    }

    public boolean isReady() {
        return isReady;
    }

    @Override
    public void childrenList(String path) {

    }

    @Override
    public void dataChanged(String path) {
        if(!isReady()) {
            tryToReadConfig();
        }
    }

    @Override
    public void nodeDeleted(String path) {

    }

    @Override
    public void reconnected() {
        this.zk = activeNodeZKListener.getZooKeeper();
        registerAliveness();
    }
}
