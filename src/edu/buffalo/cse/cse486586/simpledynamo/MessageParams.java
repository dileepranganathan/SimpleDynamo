/**
 * @author Dileep Ranganathan
 */
package edu.buffalo.cse.cse486586.simpledynamo;

import edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider.MESSAGE_TYPES;

import java.net.Socket;

public class MessageParams {
    String remotePort;
    PayLoad payload;
    MESSAGE_TYPES messageType;
    Socket clientSocket;

    public MessageParams(PayLoad payload, String remotePort, MESSAGE_TYPES msgType) {
        this.payload = payload;
        this.remotePort = remotePort;
        this.messageType = msgType;
    }

    public MessageParams(PayLoad payload, Socket tempSocket, MESSAGE_TYPES messageType) {
        // TODO Auto-generated constructor stub
        this.payload = payload;
        this.clientSocket = tempSocket;
        this.messageType = messageType;
    }

    @Override
    public String toString() {
        return "MessageParams [remotePort=" + remotePort + ", payload=(" + payload.getKey()
                + ", " + payload.getValue()
                + ", messageType=" + messageType + "]";
    }

}
