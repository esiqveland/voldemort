package voldemort.headmaster;

import com.google.common.collect.Lists;
import joptsimple.internal.Strings;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import voldemort.client.rebalance.RebalancePlan;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.headmaster.rebalance.RebalancePlannerZK;
import voldemort.headmaster.rebalance.RebalancerZK;
import voldemort.server.VoldemortConfig;


import voldemort.tools.*;
import voldemort.xml.ClusterMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class Headmaster implements Runnable, ZKDataListener {

    private static final Logger logger = LoggerFactory.getLogger(Headmaster.class);

    public static final int DEFAULT_HTTP_PORT = 6881;
    public static final int DEFAULT_ADMIN_PORT = 6667;
    public static final int DEFAULT_SOCKET_PORT = 6666;
    public static final int HEADMASTER_SIGAR_LISTENER_PORT = 17777;

    public static final String HEADMASTER_ROOT_PATH = "/headmaster";
    public static final String HEADMASTER_ELECTION_PATH = "/headmaster_";
    public static final String HEADMASTER_REBALANCE_TOKEN = "/rebalalance_token";
    private static final String HEADMASTER_UNKNOWN = "HEADMASTER_UNKNOWN";

    public static final String ACTIVEPATH = "/active";

    public static final String defaultUrl = "voldemort1.idi.ntnu.no:2181/hjemmekontor";
    public String adminUrl;

    private String myHostname;

    private ActiveNodeZKListener anzkl;

    String zkURL = defaultUrl;
    private Cluster currentCluster;
    private boolean idle = false;

    private Thread sigarThread;

    private String myHeadmaster;


    private RebalancePlan plan;
    private String currentHeadmaster;

    private ConcurrentHashMap<String, Node> handledNodes;
    private Lock currentClusterLock;

    private StatusAnalyser statAnalyser;

    public Headmaster(String zkURL, ActiveNodeZKListener activeNodeZKListener) {
        this(zkURL);
        this.anzkl = activeNodeZKListener;
        this.anzkl.addDataListener(this);

        statAnalyser = new StatusAnalyser(this, anzkl);

    }

    private Headmaster(String zkURL) {
       this.zkURL = zkURL;
       currentClusterLock = new ReentrantLock();
       handledNodes = new ConcurrentHashMap<>();
       try {
            myHostname = InetAddress.getLocalHost().getCanonicalHostName().toString();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    private void beHeadmaster() {
        logger.debug("I AM HEADMASTER");
        currentClusterLock.lock();
        try {
            currentCluster = new ClusterMapper().readCluster(anzkl.getStringFromZooKeeper("/config/cluster.xml", true));

        } finally {
            currentClusterLock.unlock();
        }
        anzkl.setWatch(HEADMASTER_ROOT_PATH+HEADMASTER_REBALANCE_TOKEN);
        //Seed childrenListChanged method with initial children list
        childrenList(ACTIVEPATH);

        if(currentCluster.getNumberOfNodes() != 0) {
            adminUrl = "tcp://" + currentCluster.getNodeById(0).getHost();
            adminUrl += ":" + currentCluster.getNodeById(0).getAdminPort();

        }
        statAnalyser.start();



    }

    public void registerAsHeadmaster(){
        String zkPath = anzkl.uploadAndUpdateFileWithMode(
                HEADMASTER_ROOT_PATH + HEADMASTER_ELECTION_PATH, myHostname+":"+HEADMASTER_SIGAR_LISTENER_PORT, CreateMode.EPHEMERAL_SEQUENTIAL);

        myHeadmaster = getNodeNameFromPath(zkPath);
        logger.debug("Registered as Headmaster in zookeeper :" + myHeadmaster);
    }

    private String getNodeNameFromPath(String zkPath) {
        String[] split = zkPath.split("/");
        return split[split.length - 1];
    }

    public boolean isHeadmaster(){
        return myHeadmaster.equals(currentHeadmaster);

    }

    private String leaderElection(){
        List<String> headmasters = anzkl.getChildrenList(HEADMASTER_ROOT_PATH);

        //determine who is supposed to be leader
        String winner = headmasters.get(0);
        long lowest_number = Long.valueOf(headmasters.get(0).split("_")[1]);

        for (String master : headmasters) {
            int sequenceNumber = new Integer(master.split("_")[1]);
            if (sequenceNumber < lowest_number) {
                lowest_number = sequenceNumber;
                winner = master;
            }
        }

        currentHeadmaster = winner;
        if (!winner.equals(myHeadmaster)){
            logger.debug("I did not win, setting watch on winner: " + HEADMASTER_ROOT_PATH+"/"+currentHeadmaster);
            anzkl.setWatch(HEADMASTER_ROOT_PATH + "/" + currentHeadmaster);
        }
        return winner;
    }

    public RebalancePlan plan (){
        // make sure of existance so we don't crash in a rebalance
        String sampleServerProperties = anzkl.getStringFromZooKeeper("/config/sample_files/server.properties");

        currentClusterLock.lock();

        try {
            RebalancePlannerZK rpzk = new RebalancePlannerZK(zkURL, anzkl);
            plan = rpzk.createRebalancePlan();
        } finally {
            currentClusterLock.unlock();
        }

        return plan;
    }

    public RebalancePlan plan (Cluster cluster){
        // make sure of existance so we don't crash in a rebalance
        String sampleServerProperties = anzkl.getStringFromZooKeeper("/config/sample_files/server.properties");

        currentClusterLock.lock();
        try {
            RebalancePlannerZK rpzk = new RebalancePlannerZK(zkURL, anzkl);
            plan = rpzk.createRebalancePlanGivenCluster(cluster);
        } finally {
            currentClusterLock.unlock();
        }

        return plan;
    }

    public void rebalance(RebalancePlan plan){
        currentClusterLock.lock();
        try {
            RebalancerZK rzk = new RebalancerZK(zkURL, adminUrl, anzkl);
            if (plan != null) {
                rzk.rebalance(plan);
            } else {
                logger.error("Rebalance called without planning being done by this headmaster beforehand");
            }
        } finally {
            currentClusterLock.unlock();
        }
    }

    @Override
    public void reconnected() {
        logger.info("got message that ZK session expiry is OVER.");
        registerAsHeadmaster();
        leaderElection();


        if(isHeadmaster()) {
            beHeadmaster();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        logger.info("Event: " + event.getType() + " path: " + event.getPath());

        if (event.getState() == Event.KeeperState.Expired) {
            stopHeadmastering();
        }

        if (event.getType() == Event.EventType.NodeCreated) {
            if (isHeadmaster() && event.getPath().equals(HEADMASTER_ROOT_PATH + HEADMASTER_REBALANCE_TOKEN)) {
                RebalancePlan plan = plan();
                try {
                    Thread.sleep(20000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                rebalance(plan);
            }
        }
    }

    private Node locateNewChildAndHandOutId(String child){
        String id = anzkl.getStringFromZooKeeper("/active/" + child);

        if (id.equals(VoldemortConfig.NEW_ACTIVE_NODE_STRING)){
            for (Node node : currentCluster.getNodes()) {

                if (node.getHost().equals(child) && !handledNodes.containsKey(child)){
                    logger.info("existing node with NEW in active: " + child + " node: " + node);
                    return node;
                }
            }
            int newId = currentCluster.getNumberOfNodes();

            Node newNode = new Node(newId, child, DEFAULT_HTTP_PORT, DEFAULT_SOCKET_PORT,DEFAULT_ADMIN_PORT,new ArrayList<Integer>());
            logger.info("New node regisering: + " + child + " : " + newId);
            return newNode;
        }

        return null;
    }

    @Override
    public synchronized void childrenList(String path) {

        if(!isHeadmaster())
            return;

        if(!path.startsWith(ACTIVEPATH)){
            //This message is for someone else
            return;
        }
        currentClusterLock.lock();
        try {
            logger.debug("Start children changed");

            List<String> children = anzkl.getChildrenList(path, true);

            HashMap<String,Node> changeMap = new HashMap<>();

            if(children.isEmpty()) {
                logger.debug("Last node left {}", ACTIVEPATH);
                return;
            }
            String members = Strings.join(children, " ");

            logger.info("{} members: {}", ACTIVEPATH, members);
            for (String child : children){
                Node newNode = locateNewChildAndHandOutId(child);
                if ( newNode != null ){
                    changeMap.put(child, newNode);
                }

            }
            if (changeMap.isEmpty()) {
                return;
            }

            String interimClusterxml = createInterimClusterXML(changeMap);
            currentCluster = new ClusterMapper().readCluster(new StringReader(interimClusterxml));


            //upload cluster.xml

            anzkl.uploadAndUpdateFile("/config/cluster.xml", interimClusterxml);

            //create node in nodes and upload server.properties
            for (Node node : changeMap.values()){
                String serverProp = createServerProperties(node);
                anzkl.uploadAndUpdateFile("/config/nodes/" + node.getHost(), "");
                anzkl.uploadAndUpdateFile("/config/nodes/" + node.getHost() + "/server.properties", serverProp);
                handledNodes.put(node.getHost(),node);
            }
        } finally {
            currentClusterLock.unlock();
        }
    }

    public void partitionMoverTrigger(String from_hostname, String to_hostname){
        Node fromNode = currentCluster.getNodeByHostname(from_hostname);
        Node toNode = currentCluster.getNodeByHostname(to_hostname);

        Cluster newCluster = moveRandomPartitionInclusterXML(fromNode,toNode);

        RebalancePlan plan = plan(newCluster);
        try {
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        rebalance(plan);
    }

    private Cluster moveRandomPartitionInclusterXML(Node fromNode, Node toNode){
        Cluster tempCluster = Cluster.cloneCluster(currentCluster);
        int stealPartitionId = 0;

        //Find partition to steal
        for(Node node : tempCluster.getNodes()){
            if(fromNode.getId() == node.getId()){
                //pick one partition and remove it from giver node
                if (!node.getPartitionIds().isEmpty()){
                    int randomPartionPosition = RandomUtils.nextInt(node.getNumberOfPartitions());
                    stealPartitionId = node.getPartitionIds().get(randomPartionPosition);

                    //Create new partitionlist
                    List partitions = Lists.newArrayList(node.getPartitionIds());
                    partitions.remove(randomPartionPosition);

                    //Create new node with one less partition
                    fromNode = new Node(node.getId(),node.getHost(),node.getHttpPort(),node.getSocketPort(),node.getAdminPort(),node.getZoneId(),partitions);
                }
            }
        }
        //Give partition
        for(Node node : tempCluster.getNodes()){
            if(toNode.getId() == node.getId()){
                //Create new partitionlist
                List partitions = Lists.newArrayList(node.getPartitionIds());
                partitions.add(stealPartitionId);

                //Create new node with one less partition
                toNode = new Node(node.getId(),node.getHost(),node.getHttpPort(),node.getSocketPort(),node.getAdminPort(),node.getZoneId(),partitions);
            }
        }
        List<Node> tempNodes = Lists.newArrayList(currentCluster.getNodes());
        List<Node> finalNodes = Lists.newArrayList();

        //Create new list of nodes
        for(Node node : tempNodes){
            if(node.getId() == fromNode.getId()){
                finalNodes.add(fromNode);
            } else if (node.getId() == toNode.getId()){
                finalNodes.add(toNode);
            } else {
                finalNodes.add(node);
            }
        }

        //Create new cluster
        Cluster newCluster = new Cluster(currentCluster.getName(), finalNodes);

        return newCluster;
    }

    private String createInterimClusterXML(HashMap<String,Node> map) {
        Collection nodeCollection = currentCluster.getNodes();

        List<Node> nodeList = Lists.newLinkedList();
        nodeList.addAll(nodeCollection);

        for(Node node : map.values()){
            if(!nodeList.contains(node)){
                nodeList.add(node);
            }
            Cluster interimCluseter = new Cluster(currentCluster.getName(),nodeList);
            String interimClusterXML = new ClusterMapper().writeCluster(interimCluseter);
            return interimClusterXML;
        }

        return new ClusterMapper().writeCluster(currentCluster);
    }

    private String createServerProperties(Node node){
        String sampleServerProp = anzkl.getStringFromZooKeeper("/config/sample_files/server.properties");

        BufferedReader br = new BufferedReader(new StringReader(sampleServerProp));

        String line;

        StringBuilder sb = new StringBuilder();
        try {
            while((line=br.readLine())!=null)
            {
                if (line.contains("node.id")){
                    sb.append("node.id=");
                    sb.append(node.getId());
                    sb.append(System.getProperty("line.separator"));
                } else {
                    sb.append(line);
                    sb.append(System.getProperty("line.separator"));
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return sb.toString();
    }



    @Override
    public void dataChanged(String path) {
        logger.info("Path changed: " + path);
        if(path.equals("/config/cluster.xml")){
            currentClusterLock.lock();
            try {
                String content = anzkl.getStringFromZooKeeper(path, true);
                Cluster newCluster = new ClusterMapper().readCluster(new StringReader(content));
                this.currentCluster = newCluster;
            } finally {
                currentClusterLock.unlock();
            }
        }
    }

    private void stopHeadmastering() {
        currentHeadmaster = HEADMASTER_UNKNOWN;
        myHeadmaster = null;
        handledNodes = new ConcurrentHashMap<>();
        statAnalyser.stop();

    }

    @Override
    public void nodeDeleted(String path) {
        logger.debug("node deleted " + path);
        if(path.equals(HEADMASTER_ROOT_PATH + "/" + currentHeadmaster)){
            //Leader has died, run new election
            leaderElection();

            if(isHeadmaster()){
                beHeadmaster();
            }
        }

    }

    public void setIdle() {
        synchronized (this) {
            this.idle = true;
            // Causes all waiters to wake up.
            this.notifyAll();
        }
    }
    @Override
    public void run() {
        synchronized (this) {

            while (true) {
                // If the flag is set, we're done.
                if (this.idle) {
                    break;
                }
                // Go to sleep until another thread notifies us.
                try {
                    this.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    setIdle();
                }
            }
        }
    }

    public String getMyHeadmaster() {
        return myHeadmaster;
    }

    public String getCurrentHeadmaster() {
        return currentHeadmaster;
    }

    public static void main(String args[]) {

        String url = defaultUrl;
        if (args.length == 0) {
            System.out.println(
                    String.format(
                            "usage: %s [zookeeperurl]\nDefaults to %s", Headmaster.class.getCanonicalName(), defaultUrl)
            );
        } else {
            url = args[0];
        }

        ActiveNodeZKListener activeNodeZKListener = new ActiveNodeZKListener(url);
        Headmaster headmaster = new Headmaster(url, activeNodeZKListener);

        Thread worker = new Thread(headmaster);
        worker.start();
    }


}

