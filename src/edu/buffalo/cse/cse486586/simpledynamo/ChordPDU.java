/**
 * @author Dileep Ranganathan
 */
package edu.buffalo.cse.cse486586.simpledynamo;

import edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider.MESSAGE_TYPES;

import java.io.Serializable;

public class ChordPDU implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = -2307410862657495780L;
    MESSAGE_TYPES messageType;
    PayLoad payload;

    public ChordPDU(MESSAGE_TYPES messageType, PayLoad payload) {
        this.payload = payload;
        this.messageType = messageType;
    }

}
