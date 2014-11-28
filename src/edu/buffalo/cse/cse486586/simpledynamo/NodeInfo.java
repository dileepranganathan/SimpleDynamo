/**
 * @author Dileep Ranganathan
 */
package edu.buffalo.cse.cse486586.simpledynamo;

public class NodeInfo {

    private String nodeId;
    private String portId;
    private String successorChord1Id;
    private String successorChord2Id;

    public NodeInfo(String nodeId, String portId) {
        super();
        this.nodeId = nodeId;
        this.portId = portId;
    }

    public NodeInfo(String nodeId, String portId, String successorChord1Id, String successorChord2Id) {
        super();
        this.nodeId = nodeId;
        this.portId = portId;
        this.setSuccessorChord1Id(successorChord1Id);
        this.setSuccessorChord2Id(successorChord2Id);
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getPortId() {
        return portId;
    }

    public void setPortId(String portId) {
        this.portId = portId;
    }

    public String getSuccessorChord1Id() {
        return successorChord1Id;
    }

    public void setSuccessorChord1Id(String successorChord1Id) {
        this.successorChord1Id = successorChord1Id;
    }

    public String getSuccessorChord2Id() {
        return successorChord2Id;
    }

    public void setSuccessorChord2Id(String successorChord2Id) {
        this.successorChord2Id = successorChord2Id;
    }

}
