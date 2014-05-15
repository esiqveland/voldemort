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
    String zkURL = "voldemort1.idi.ntnu.no/hjemmekontor";

    AdminClientConfig adminClientConfig;
    ClientConfig clientConfig;
    AdminClient adminClient;
    ActiveNodeZKListener anzkl;

    public AdminToolZK(String voldemortAdminConnectionURL, String zkURL, ActiveNodeZKListener anzkl){
        this.voldemortAdminConnectionURL = voldemortAdminConnectionURL;
        this.zkURL = zkURL;
        this.anzkl = anzkl;

        //Creates configs with default values
        adminClientConfig = new AdminClientConfig();
        clientConfig = new ClientConfig();

        adminClient = new AdminClient(voldemortAdminConnectionURL, adminClientConfig, clientConfig);

    }

    public void repairJob(int nodeId){
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
