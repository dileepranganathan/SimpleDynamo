/**
 * @author Dileep Ranganathan
 */
package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

public class Node implements Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = 4219333563278149684L;
    String node_id;
    String chord_id;
    Map<String, NodeInfo> dynamoRing;

    public Node(String node_id, String chord_id) {
        this.node_id = node_id;
        this.chord_id = chord_id;
        dynamoRing = new TreeMap<String, NodeInfo>();
    }

    public NodeInfo locateNodeInRing(String key) {
        String firstKey = null;
        boolean isFirst = true;
        for (String keyNode : dynamoRing.keySet()) {
            if (isFirst) {
                firstKey = keyNode;
                isFirst = false;
            }
            if (key.compareTo(keyNode) <= 0) {
                return dynamoRing.get(keyNode);
            }
        }
        return dynamoRing.get(firstKey);
    }

}
