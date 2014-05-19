package voldemort.headmaster;

import org.joda.time.Seconds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import voldemort.client.rebalance.RebalancePlan;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.headmaster.sigar.NodeStatus;
import voldemort.headmaster.sigar.SigarReceiver;
import voldemort.headmaster.sigar.SigarStatusMessage;
import voldemort.headmaster.sigar.StatusMessageListener;
import voldemort.xml.ClusterMapper;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class StatusAnalyser implements StatusMessageListener, Runnable{

    private static final Logger logger = LoggerFactory.getLogger(StatusAnalyser.class);
    public static final double CPU_THRESHHOLD = 0.80;

    private Headmaster headmaster;
    private SigarReceiver sigarReceiver;
    private ActiveNodeZKListener anzkl;
    private Cluster currentCluster;
    private Thread sigarThread;
    private boolean performingOperation;
    private boolean activeListening;

    private HashMap<String, LinkedList<SigarStatusMessage>> messagesFromNodes;
    private ScheduledExecutorService scheduler;


    public StatusAnalyser(Headmaster headmaster, ActiveNodeZKListener anzkl) {

        messagesFromNodes = new HashMap<>();
        performingOperation = false;
        this.headmaster = headmaster;
        this.anzkl = anzkl;
        scheduler = Executors.newScheduledThreadPool(1);

        sigarReceiver= new SigarReceiver(Headmaster.HEADMASTER_SIGAR_LISTENER_PORT,this);
        sigarThread = new Thread(sigarReceiver);
        sigarThread.start();

    }


    @Override
    public void statusMessage(SigarStatusMessage sigarStatusMessage) {
        if (!headmaster.isHeadmaster()) {
            return;
        }

        addStatusMessage(sigarStatusMessage);

        logger.info("Received message: {}", sigarStatusMessage.toString());

    }

    private void addStatusMessage(SigarStatusMessage msg){

        if (!messagesFromNodes.containsKey(msg)){
            LinkedList<SigarStatusMessage> newList = new LinkedList<>();
            newList.add(msg);
            messagesFromNodes.put(msg.getHostname(),newList);
        } else {

            LinkedList<SigarStatusMessage> entry = messagesFromNodes.get(msg.getHostname());
            if (entry.size() < 10) {
                entry.add(msg);
            } else {
                entry.remove(entry.getFirst());
                entry.add(msg);
            }
        }
    }

    private double getAvgCPU(String hostname) {
        LinkedList<SigarStatusMessage> entry = messagesFromNodes.get(hostname);

        if (entry.isEmpty()) {
            throw new NoSuchElementException("No recorded messages for that hostname!");
        } else {
            double sum = 0;
            for (SigarStatusMessage ssm : entry) {
                sum += ssm.getCPU();
            }
            return sum / entry.size();
        }
    }

    private double getAvgCPU(List<SigarStatusMessage> cpuEntries) {
        if (cpuEntries.isEmpty()) {
            throw new NoSuchElementException("No recorded messages for that hostname!");
        } else {
            double sum = 0;
            for (SigarStatusMessage ssm : cpuEntries) {
                sum += ssm.getCPU();
            }
            return sum / cpuEntries.size();


        }
    }

    private String getCalmestNode(){
        String lowestHostname = "";
        double lowestAvg = 1000;
        for (LinkedList<SigarStatusMessage> node_messages : messagesFromNodes.values()){
            double avgCpu = getAvgCPU(node_messages);
            if(avgCpu < lowestAvg){
                lowestAvg = avgCpu;
                lowestHostname = node_messages.getFirst().getHostname();
            }
        }
        return lowestHostname;
    }

    private void analyseCPU (){
        logger.info("Performing cluster analysis");
        for (LinkedList<SigarStatusMessage> node_messages : messagesFromNodes.values()){
            double avgCpu = getAvgCPU(node_messages);
            if (avgCpu > CPU_THRESHHOLD) {
                String strugglingHost = node_messages.getFirst().getHostname();
                logger.debug("Node: {} is struggling with AVGCPU: {}! ", strugglingHost, avgCpu);

                String calm_node = getCalmestNode();
                double calmCPU = getAvgCPU(calm_node);

                if (calmCPU < 70) {
                    if (calm_node.equals(strugglingHost)){
                        logger.debug("Sanitycheck failed: Calm node is same as struggling");
                        return;
                    }
                    logger.debug("Initiating partition move from {} to {}",calm_node, strugglingHost);
                    if (!performingOperation){
                        performingOperation = true;
                        try {
                            headmaster.partitionMoverTrigger(getCalmestNode(), strugglingHost);
                        } catch (Exception e){
                            logger.error("Error while moving partition",e);
                        }
                    }
                    performingOperation = false;
                }

            }
        }
    }

    @Override
    public void run() {
        analyseCPU();
    }


    public void stop() {
        scheduler.shutdown();
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this,0,60, TimeUnit.SECONDS);
    }
}
