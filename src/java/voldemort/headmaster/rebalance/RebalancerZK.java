package voldemort.headmaster.rebalance;

import voldemort.client.rebalance.RebalancePlan;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.headmaster.ActiveNodeZKListener;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.RebalanceUtils;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import java.io.StringReader;
import java.util.List;

public class RebalancerZK {

    private String bootstrapUrl;
    private String zkUrl;
    private ActiveNodeZKListener zkHandler;

    //Rebalance parameters
    private int parallelism = 8;
    private long proxyPauseSec = 30;
    private String outputDir = "rebalance-out";


    public RebalancerZK(String zkUrl, String bootstrapUrl, ActiveNodeZKListener zkHandler){
        this.bootstrapUrl = bootstrapUrl;
        this.zkUrl = zkUrl;
        this.zkHandler = zkHandler;
    }

    public void rebalance(RebalancePlan plan){
        RebalanceControllerZK rebalanceController;
        rebalanceController = new RebalanceControllerZK(bootstrapUrl,
                parallelism,
                proxyPauseSec);

        //Get current cluster from voldemort nodes
        Cluster currentCluster = rebalanceController.getCurrentCluster();
        List<StoreDefinition> currentStoreDefs = rebalanceController.getCurrentStoreDefs();

        // If this test doesn't pass, something is wrong in prod!
        RebalanceUtils.validateClusterStores(currentCluster, currentStoreDefs);

        //Fetch configfiles from ZooKeeper
        Cluster finalCluster = plan.getFinalCluster();
        List<StoreDefinition> finalStoreDefs = plan.getFinalStores();


        //Validate stores across clusters
        RebalanceUtils.validateClusterStores(finalCluster, finalStoreDefs);
        RebalanceUtils.validateCurrentFinalCluster(currentCluster, finalCluster);

        int batchSize = Integer.MAX_VALUE;

        // Plan & execute rebalancing.
        boolean failure = false;
        try{
            rebalanceController.rebalance(plan);
        } catch (Exception e){
            failure = true;
        }

        if(failure){
            //Issue rollback of cluster.xml
            zkHandler.uploadAndUpdateFile("/config/cluster.xml", new ClusterMapper().writeCluster(plan.getCurrentCluster()));

            //RollBack state
            for (Node nodes : finalCluster.getNodes()){
                zkHandler.uploadAndUpdateFile("/config/nodes/"+nodes.getHost()+"/server.state", MetadataStore.VoldemortState.NORMAL_SERVER.toString());
            }
        } else {
            zkHandler.uploadAndUpdateFile("/config/cluster.xml",new ClusterMapper().writeCluster(finalCluster));
        }
    }

}


