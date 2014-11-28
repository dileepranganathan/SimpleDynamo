/**
 * @author Dileep Ranganathan
 */
package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;

public class DHTStore implements Serializable {

    private static final long serialVersionUID = -363688468786428306L;
    private String key;
    private String value;
    private String version;
    private String node;

    public DHTStore(String key, String value, String version, String node) {
        super();
        this.key = key;
        this.value = value;
        this.version = version;
        this.node = node;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getNode() {
        return node;
    }

    public void setNode(String node) {
        this.node = node;
    }
}
