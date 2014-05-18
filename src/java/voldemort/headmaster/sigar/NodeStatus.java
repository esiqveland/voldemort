package voldemort.headmaster.sigar;

import voldemort.cluster.Node;

public class NodeStatus extends Node {

    private Double cpustatus;
    private Double memstatus;
    private Double diskstatus;

    public NodeStatus(Node node) {
        super(node.getId(), node.getHost(), node.getHttpPort(), node.getSocketPort(), node.getAdminPort(),
                node.getZoneId(), node.getPartitionIds(), node.getRestPort());
    }

    public Double getCpustatus() {
        return cpustatus;
    }

    public void setCpustatus(Double cpustatus) {
        this.cpustatus = cpustatus;
    }

    public Double getMemstatus() {
        return memstatus;
    }

    public void setMemstatus(Double memstatus) {
        this.memstatus = memstatus;
    }

    public Double getDiskstatus() {
        return diskstatus;
    }

    public void setDiskstatus(Double diskstatus) {
        this.diskstatus = diskstatus;
    }
}
