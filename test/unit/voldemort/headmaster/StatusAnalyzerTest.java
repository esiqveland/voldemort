package voldemort.headmaster;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.xml.ClusterMapper;

import java.util.List;


/**
 * Created by Knut on 22/05/14.
 */
public class StatusAnalyzerTest {

    private static String EXAMPLE_CLUSTER =
            "<cluster>\n" +
                    "        <name>mycluster</name>\n" +
                    "        <server>\n" +
                    "                <id>0</id>\n" +
                    "                <host>192.168.0.104</host>\n" +
                    "                <http-port>8081</http-port>\n" +
                    "                <socket-port>6666</socket-port>\n" +
                    "                <partitions>0, 1</partitions>\n" +
                    "        </server>\n" +
                    "        <server>\n" +
                    "                <id>1</id>\n" +
                    "                <host>192.168.0.105</host>\n" +
                    "                <http-port>8081</http-port>\n" +
                    "                <socket-port>6666</socket-port>\n" +
                    "                <partitions>2,3</partitions>\n" +
                    "        </server>\n" +
                    "        <server>\n" +
                    "                <id>2</id>\n" +
                    "                <host>192.168.0.106</host>\n" +
                    "                <http-port>8081</http-port>\n" +
                    "                <socket-port>6666</socket-port>\n" +
                    "                <partitions></partitions>\n" +
                    "        </server>\n" +
            "</cluster>\n";

    @Test
    public void moveOnePartitionTest(){
        Cluster currentCluster = new ClusterMapper().readCluster(EXAMPLE_CLUSTER);

        List<Node> oldStrugglingNodes = Lists.newArrayList(currentCluster.getNodeById(0));
        Node oldCalmNode = currentCluster.getNodeById(2);

        Cluster newCluster;
        newCluster = PartitionMovePlanner.movePartitonsInClusterObject(oldStrugglingNodes,oldCalmNode,currentCluster);

        Node newNode0 = newCluster.getNodeById(0);
        Node newNode1 = newCluster.getNodeById(1);
        Node newCalmNode = newCluster.getNodeById(2);

        Assert.assertTrue((newNode0.getPartitionIds().size() == 1) && (newNode1.getPartitionIds().size() == 2));
        Assert.assertTrue(newCalmNode.getPartitionIds().size() == 1);
        Assert.assertFalse(newNode0.getPartitionIds().contains(newCalmNode.getPartitionIds().get(0)));

    }

    @Test
    public void moveAllPartitionTest(){
        Cluster currentCluster = new ClusterMapper().readCluster(EXAMPLE_CLUSTER);

        List<Node> oldStrugglingNodes = Lists.newArrayList(currentCluster.getNodeById(0),currentCluster.getNodeById(1));
        Node oldCalmNode = currentCluster.getNodeById(2);

        Cluster newCluster;
        newCluster = PartitionMovePlanner.movePartitonsInClusterObject(oldStrugglingNodes,oldCalmNode,currentCluster);

        Node newNode0 = newCluster.getNodeById(0);
        Node newNode1 = newCluster.getNodeById(1);
        Node newCalmNode = newCluster.getNodeById(2);


        //Verify that one partiton has moved from both struggling nodes
        Assert.assertTrue((newNode0.getPartitionIds().size() == 1) || (newNode1.getPartitionIds().size() == 1));

        //Verify that calm node has received 2 partitions
        Assert.assertTrue(newCalmNode.getPartitionIds().size() == 2);


        //Verify that there is no overlap between partitions
        for(int partition : newCalmNode.getPartitionIds()){
            Assert.assertFalse(newNode0.getPartitionIds().contains(partition));
            Assert.assertFalse(newNode1.getPartitionIds().contains(partition));
        }
    }

}
