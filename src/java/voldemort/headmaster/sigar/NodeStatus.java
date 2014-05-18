package voldemort.headmaster.sigar;

import voldemort.cluster.Node;

public class NodeStatus extends Node {

    private float cpustatus;
    private float memstatus;
    private float diskstatus;

    public NodeStatus(Node node) {
        super(node.getId(), node.getHost(), node.getHttpPort(), node.getSocketPort(), node.getAdminPort(),
                node.getZoneId(), node.getPartitionIds(), node.getRestPort());
    }

    public float getCpustatus() {
        return cpustatus;
    }

    public void setCpustatus(float cpustatus) {
        this.cpustatus = cpustatus;
    }

    public float getMemstatus() {
        return memstatus;
    }

    public void setMemstatus(float memstatus) {
        this.memstatus = memstatus;
    }

    public float getDiskstatus() {
        return diskstatus;
    }

    public void setDiskstatus(float diskstatus) {
        this.diskstatus = diskstatus;
    }

}
