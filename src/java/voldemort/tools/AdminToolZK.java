package voldemort.tools;

import voldemort.VoldemortAdminTool;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.headmaster.ActiveNodeZKListener;
import voldemort.store.StoreDefinition;
import voldemort.utils.RebalanceUtils;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import java.io.StringReader;
import java.util.List;

/**
 * Created by knut on 15/05/14.
 */
public class AdminToolZK {

    String voldemortAdminConnectionURL = "tcp://192.168.0.110:6667";

    AdminClientConfig adminClientConfig;
    ClientConfig clientConfig;
    AdminClient adminClient;
    ActiveNodeZKListener anzkl;

    public AdminToolZK(String voldemortAdminConnectionURL, ActiveNodeZKListener anzkl){
        this.voldemortAdminConnectionURL = voldemortAdminConnectionURL;
        this.anzkl = anzkl;

        //Creates configs with default values
        adminClientConfig = new AdminClientConfig();
        clientConfig = new ClientConfig();
    }

    public void repairJob(int nodeId){

        adminClient = new AdminClient(voldemortAdminConnectionURL, adminClientConfig, clientConfig);

        String currentClusterXML = anzkl.getStringFromZooKeeper("/config/cluster.xml");
        Cluster currentCluster = new ClusterMapper().readCluster(new StringReader(currentClusterXML));

        if(nodeId < 0) {
            for(Node node: currentCluster.getNodes()) {
                adminClient.storeMntOps.repairJob(node.getId());
            }
        } else {
            adminClient.storeMntOps.repairJob(nodeId);
        }
    }

}
