package voldemort.headmaster;

import com.google.common.collect.Lists;
import org.apache.commons.lang.math.RandomUtils;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;

import java.util.List;

/**
 * Created by Knut on 22/05/14.
 */
public class PartitionMovePlanner {

    private Cluster currentCluster;
    private Headmaster headmaster;

    public PartitionMovePlanner (){

    }

    /**
     *
     * @param strugglingNode set of nodes to move a partition from
     * @param calmNode the node to receive said partitions
     * @return an updated cluster object
     */

    public static Cluster movePartitonsInClusterObject(List<Node> strugglingNodes, Node calmNode, Cluster currentCluster) {

        Cluster tempCluster = Cluster.cloneCluster(currentCluster);

        List<Integer> stealPartitionIds = Lists.newArrayList();
        List<Node> localStrugglingNodes = Lists.newArrayList();

        //Find partition to steal
        for (Node node : tempCluster.getNodes()) {
            for (Node strugglingNode : strugglingNodes) {

                if (strugglingNode.getId() == node.getId()) {
                    //pick one partition and remove it from giver node
                    if (node.getPartitionIds().isEmpty()) {
                        continue;
                    }
                    int randomPartionPosition = RandomUtils.nextInt(node.getNumberOfPartitions());
                    stealPartitionIds.add(node.getPartitionIds().get(randomPartionPosition));

                    //Create new partitionlist
                    List partitions = Lists.newArrayList(node.getPartitionIds());
                    partitions.remove(randomPartionPosition);

                    //Create new node with one less partition
                    localStrugglingNodes.add(new Node(node.getId(), node.getHost(), node.getHttpPort(), node.getSocketPort(), node.getAdminPort(), node.getZoneId(), partitions));
                }
            }
        }
        //Give partition to calm node
        Node updatedCalmNode = null;
        for (Node node : tempCluster.getNodes()) {
            if (calmNode.getId() == node.getId()) {
                //Create new partitionlist
                List partitions = Lists.newArrayList(node.getPartitionIds());
                for (int stealIds : stealPartitionIds) {
                    partitions.add(stealIds);
                }

                //Create new node with one less partition
                updatedCalmNode = new Node(node.getId(), node.getHost(), node.getHttpPort(), node.getSocketPort(), node.getAdminPort(), node.getZoneId(), partitions);
            }
        }
        List<Node> tempNodes = Lists.newArrayList(currentCluster.getNodes());
        List<Node> finalNodes = Lists.newArrayList();

        //Create new list of nodes
        for (Node node : tempNodes) {


            boolean bool = false;
            for(Node strugg : localStrugglingNodes) {
                if (node.getId() == strugg.getId()) {
                    finalNodes.add(strugg);
                    bool = true;
                }
            }

            if(bool) {
                continue;
            }

            if (node.getId() == calmNode.getId()) {
                finalNodes.add(updatedCalmNode);
            } else {
                finalNodes.add(node);
            }


        }
        //Create new cluster
        Cluster newCluster = new Cluster(currentCluster.getName(), finalNodes);

        return newCluster;
    }
}
