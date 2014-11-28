/**
 * @author Dileep Ranganathan
 */
package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleDynamoProvider extends ContentProvider {
    private DynamoDBHelper database;
    static final String TAG = "SimpleDynamoProvider";
    private Map<String, Map<String, DHTStore>> queryResponseQueue;
    private Map<String, Map<String, DHTStore>> queryAllResponseQueue;
    private BlockingQueue<Integer> deleteResponseQueue;
    // private Map<String, List<Integer>> concurrentMap = new
    // ConcurrentHashMap<String, List<Integer>>();
    private Map<String, Integer> concurrentMap = new ConcurrentHashMap<String, Integer>();
    // private BlockingQueue<Map<String, String>> reHashQueue;
    static final int SERVER_PORT = 10000;
    private Node nodePtr;
    private AtomicInteger atomicRequestId = new AtomicInteger(0);
    private Object insertLock = new Object();
    private Object queryLock = new Object();
    static final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
    static final String[] remotePorts = {
            "5554", "5556", "5558", "5560", "5562"
    };

    public enum MESSAGE_TYPES implements Serializable {
        // "join", "insert", "node", "predecessor", "query"
        INSERT_RESP, INSERT_NOW, QUERY, QUERY_ALL, QUERY_RESP, DELETE, DELETE_ALL, DELETE_RESP, RECOVER, INSERT_REPLICA1, INSERT_REPLICA2,
        QUERY_REPLICA1, QUERY_REPLICA2, DELETE_REPLICA1, DELETE_REPLICA2
    };

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        TelephonyManager tel = (TelephonyManager) getContext()
                .getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(
                tel.getLine1Number().length() - 4);
        queryResponseQueue = new ConcurrentHashMap<String, Map<String, DHTStore>>();
        queryAllResponseQueue = new ConcurrentHashMap<String, Map<String, DHTStore>>();
        deleteResponseQueue = new LinkedBlockingQueue<Integer>();

        String node_id;
        String recoveryPorts[] = new String[3];
        try {
            node_id = this.genHash(portStr);
            nodePtr = new Node(node_id, portStr);
            int i = 1;
            int len = remotePorts.length;
            List<String> ringList = new ArrayList<String>(5);
            for (String remotePort : remotePorts) {
                String ringNodeId = this.genHash(remotePort);
                ringList.add(ringNodeId);
                nodePtr.dynamoRing.put(ringNodeId, new NodeInfo(ringNodeId, remotePort));
            }
            Collections.sort(ringList);
            for (String keyNode : ringList) {
                NodeInfo ringNode = nodePtr.dynamoRing.get(keyNode);
                NodeInfo sucNode1 = nodePtr.dynamoRing.get(ringList.get(i % len));
                NodeInfo sucNode2 = nodePtr.dynamoRing.get(ringList.get((i + 1) % len));
                nodePtr.dynamoRing.remove(keyNode);
                nodePtr.dynamoRing.put(keyNode, new NodeInfo(ringNode.getNodeId(),
                        ringNode.getPortId(),
                        sucNode1.getPortId(), sucNode2.getPortId()));
                if (keyNode.equals(node_id)) {
                    NodeInfo recNode1 = nodePtr.dynamoRing.get(ringList.get(((i - 3) + len) % len));
                    NodeInfo recNode2 = nodePtr.dynamoRing.get(ringList.get(((i - 2) + len) % len));
                    NodeInfo recNode3 = nodePtr.dynamoRing.get(ringList.get((i + 1) % len));
                    recoveryPorts[0] = recNode2.getPortId();
                    recoveryPorts[1] = recNode1.getPortId();
                    recoveryPorts[2] = recNode3.getPortId();
                    Log.e(TAG, "Coordinator::::" + ringNode.getPortId());
                    Log.e(TAG, "SuccerReplica1::::" + recoveryPorts[2]);
                    Log.e(TAG, "Predecessor1::::" + recoveryPorts[0]);
                    Log.e(TAG, "Predecessor2::::" + recoveryPorts[1]);
                }
                i++;
            }
            File dbFile = this.getContext().getDatabasePath(DynamoDBHelper.DATABASE_NAME);
            database = new DynamoDBHelper(getContext());
            if (dbFile.exists()) {
                // RECOVERY BEGINS

                // Delete current
                SQLiteDatabase db = database.getWritableDatabase();
                // db.delete(LocalDHTable.TABLE_DYNAMO, null, null);
                // getContext().getContentResolver().notifyChange(mUri, null);

                PayLoad payload0 = new PayLoad();
                payload0.setQueryInitiator(portStr);
                // Predecessor 1
                payload0.setKey(recoveryPorts[0]);
                Integer remPort = Integer.parseInt(recoveryPorts[0]) * 2;
                String remotePortNo = remPort.toString();
                MessageParams recMsg = new MessageParams(payload0, remotePortNo,
                        MESSAGE_TYPES.RECOVER);
                new Thread(new RecoveryTask(recMsg)).start();

                PayLoad payload1 = new PayLoad();
                payload1.setQueryInitiator(portStr);
                // Predecessor 2
                payload1.setKey(recoveryPorts[1]);
                remPort = Integer.parseInt(recoveryPorts[1]) * 2;
                remotePortNo = remPort.toString();
                recMsg = new MessageParams(payload1, remotePortNo, MESSAGE_TYPES.RECOVER);
                new Thread(new RecoveryTask(recMsg)).start();

                PayLoad payload2 = new PayLoad();
                payload2.setQueryInitiator(portStr);
                // Successor 1
                payload2.setKey(portStr); // Mine
                remPort = Integer.parseInt(recoveryPorts[2]) * 2;
                remotePortNo = remPort.toString();
                recMsg = new MessageParams(payload2, remotePortNo, MESSAGE_TYPES.RECOVER);
                new Thread(new RecoveryTask(recMsg)).start();
            }
        } catch (NoSuchAlgorithmException e) {
            // TODO Auto-generated catch block
            Log.e(TAG, e.toString());
        }
        createServerSocket();
        return false;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        try {
            String key = genHash(values.getAsString(LocalDHTable.COLUMN_KEY).trim());
            NodeInfo nodeInfo = nodePtr.locateNodeInRing(key);
            // synchronized (insertNodeLock) {
            if (nodeInfo.getPortId().equals(nodePtr.chord_id)) {
                insertToDHT(mUri, values);
            } else {
                sendInsertPDU(nodeInfo.getPortId(), values, MESSAGE_TYPES.INSERT_NOW);
            }
        } catch (NoSuchAlgorithmException e) {
            // TODO Auto-generated catch block
            Log.e(TAG, "Insertion Failed!!!");
            Log.e(TAG, e.toString());
        }
        return null;
    }

    public void sendInsertPDU(String portId, ContentValues values, MESSAGE_TYPES mesgType) {
        Integer remPort = Integer.parseInt(portId) * 2;
        String remotePortNo = remPort.toString();
        String nKey = values.getAsString(LocalDHTable.COLUMN_KEY).trim();
        String nValue = values.getAsString(LocalDHTable.COLUMN_VALUE).trim();
        PayLoad payload = new PayLoad();
        payload.setKey(nKey);
        payload.setValue(nValue);
        payload.setVersion(0);
        MessageParams nodeMsg = new MessageParams(payload, remotePortNo,
                mesgType);
        Log.d(TAG, nodeMsg.toString());
        new Thread(new ClientTask(nodeMsg)).start();
    }

    public Uri insertToDHT(Uri uri, ContentValues values) {
        synchronized (insertLock) {
            long id = 0;
            ContentValues cv = new ContentValues(values);
            cv.put(LocalDHTable.COLUMN_VERSION, 0);
            try {
                NodeInfo nodeInfo1 = nodePtr.locateNodeInRing(genHash(values.getAsString(
                        LocalDHTable.COLUMN_KEY).trim()));
                cv.put(LocalDHTable.COLUMN_NODE, nodeInfo1.getPortId());
                SQLiteDatabase sqlDB = database.getWritableDatabase();
                id = sqlDB.replace(LocalDHTable.TABLE_DYNAMO, null, cv);
                if (id > 0) {
                    Uri newUri = ContentUris.withAppendedId(uri, id);
                    getContext().getContentResolver().notifyChange(newUri, null);
                    concurrentMap.put(values.getAsString(LocalDHTable.COLUMN_KEY).trim(), 0);

                    Log.d(TAG,
                            "COORD_REPLICA :: COORDINATOR for this key is : "
                                    + nodeInfo1.getPortId()
                                    + ", SUCC1 : " + nodeInfo1.getSuccessorChord1Id()
                                    + ", SUCC2 : " + nodeInfo1.getSuccessorChord2Id());
                    sendInsertPDU(nodeInfo1.getSuccessorChord1Id(), cv,
                            MESSAGE_TYPES.INSERT_REPLICA1);

                    return newUri;

                } else {
                    Log.e(TAG, "Insertion Failed!!!");
                }
            } catch (NoSuchAlgorithmException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return null;
        }
    }

    public Uri insertReplicaToDHT(Uri uri, ContentValues values, MESSAGE_TYPES mesgType) {
        synchronized (insertLock) {
            long id = 0;
            ContentValues cv = new ContentValues(values);
            cv.put(LocalDHTable.COLUMN_VERSION, 0);
            try {
                NodeInfo nodeInfo2 = nodePtr.locateNodeInRing(genHash(values
                        .getAsString(LocalDHTable.COLUMN_KEY).trim()));
                cv.put(LocalDHTable.COLUMN_NODE, nodeInfo2.getPortId());
                SQLiteDatabase sqlDB = database.getWritableDatabase();
                id = sqlDB.replace(LocalDHTable.TABLE_DYNAMO, null, cv);
                if (id > 0) {
                    Uri newUri = ContentUris.withAppendedId(uri, id);
                    getContext().getContentResolver().notifyChange(newUri, null);
                    concurrentMap.put(values.getAsString(LocalDHTable.COLUMN_KEY).trim(), 0);
                    Log.d(TAG, "INSERT REPLICATED externally in Node:" + nodePtr.chord_id);
                    if (mesgType == MESSAGE_TYPES.INSERT_REPLICA1) {
                        sendInsertPDU(nodeInfo2.getSuccessorChord2Id(), cv,
                                MESSAGE_TYPES.INSERT_REPLICA2);
                    }
                    return newUri;
                } else {
                    Log.e(TAG, "Insertion Failed!!!");
                }
            } catch (NoSuchAlgorithmException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return null;
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
            String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub
        SQLiteDatabase db = database.getWritableDatabase();
        String queryStr = "select * from " + LocalDHTable.TABLE_DYNAMO;
        selection = selection.trim();
        if (selection.equalsIgnoreCase("@")) {
            queryStr = "select key,value from " + LocalDHTable.TABLE_DYNAMO;
            Cursor cursor = db.rawQuery(queryStr, null);
            return cursor;
        } else {
            try {
                if (selection.equalsIgnoreCase("*")) {
                    queryStr = "select * from " + LocalDHTable.TABLE_DYNAMO;
                    Cursor cursor = db.rawQuery(queryStr, null);
                    NodeInfo queryAllNodes = nodePtr.dynamoRing.get(nodePtr.node_id);
                    Integer remPort = Integer.parseInt(queryAllNodes.getSuccessorChord1Id()) * 2;
                    String remotePortNo = remPort.toString();
                    PayLoad payload = new PayLoad();
                    payload.setKey(selection);
                    payload.setQueryInitiator(nodePtr.chord_id);
                    payload.setQueryCursor(cursor);
                    payload.setValue("KEY");
                    MessageParams nodeMsg = new MessageParams(payload, remotePortNo,
                            MESSAGE_TYPES.QUERY_ALL);
                    new Thread(new ClientTask(nodeMsg)).start();
                    cursor.close();
                    return generateQueryAllResponse(selection);
                } else {
                    queryStr += " where key like '" + selection + "'";
                    String key = genHash(selection);
                    String rqId = String.valueOf(atomicRequestId.incrementAndGet());
                    NodeInfo nodeInfo = nodePtr.locateNodeInRing(key);
                    if (nodeInfo.getSuccessorChord2Id().equals(nodePtr.chord_id)) {
                        PayLoad payload = new PayLoad();
                        payload.setKey(selection);
                        payload.setValue(nodePtr.chord_id);
                        payload.setReqId(rqId);
                        ChordPDU chordPDU = new ChordPDU(MESSAGE_TYPES.QUERY, payload);
                        queryDHT(chordPDU);
                    } else {
                        sendQueryPDU(nodeInfo.getSuccessorChord2Id(), selection,
                                MESSAGE_TYPES.QUERY, rqId);
                    }
                    return generateQueryResponse(nodeInfo, selection, rqId);
                }
            } catch (NoSuchAlgorithmException nsae) {
                Log.e(TAG, nsae.toString());
            }
        }
        return null;
    }

    public void sendQueryPDU(String portId, String selection, MESSAGE_TYPES mesgType, String reqId) {
        Integer remPort = Integer.parseInt(portId) * 2;
        String remotePortNo = remPort.toString();
        PayLoad payload = new PayLoad();
        payload.setKey(selection);
        payload.setValue(nodePtr.chord_id);
        payload.setReqId(reqId);
        MessageParams nodeMsg = new MessageParams(payload, remotePortNo, mesgType);
        new Thread(new ClientTask(nodeMsg)).start();
    }

    public void queryDHT(ChordPDU queryPDU) {
        String queryStr = "select * from " + LocalDHTable.TABLE_DYNAMO;
        if (queryPDU.messageType == MESSAGE_TYPES.QUERY_ALL) {
            SQLiteDatabase db = database.getWritableDatabase();
            if (nodePtr.chord_id.equals(queryPDU.payload.getQueryInitiator())) {
                Log.d("DSPOCK", "Query_all ring completed... Count > "
                        + queryPDU.payload.getQueryCursor().size());
                queryAllResponseQueue.put(queryPDU.payload.getKey(),
                        queryPDU.payload.getQueryCursor());
            } else {
                Cursor cursor = db.rawQuery(queryStr, null);
                NodeInfo queryAllNodes = nodePtr.dynamoRing.get(nodePtr.node_id);
                Integer remPort = Integer.parseInt(queryAllNodes.getSuccessorChord1Id()) * 2;
                String remotePortNo = remPort.toString();
                queryPDU.payload.mergeCursor(cursor);
                MessageParams nodeMsg = new MessageParams(queryPDU.payload, remotePortNo,
                        MESSAGE_TYPES.QUERY_ALL);
                new Thread(new ClientTask(nodeMsg)).start();
                cursor.close();
            }
        } else {
            queryStr += " where key like '"
                    + queryPDU.payload.getKey() + "'";
            Cursor cursor;
            while (!concurrentMap.containsKey(queryPDU.payload.getKey())) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            synchronized (queryLock) {
                SQLiteDatabase db = database.getWritableDatabase();
                cursor = db.rawQuery(queryStr, null);
                // make sure that potential listeners are getting notified
                cursor.setNotificationUri(getContext().getContentResolver(), mUri);
            }
            // Send Query Response if successful
            Integer remPort = Integer.parseInt(queryPDU.payload.getValue()) * 2;
            String remotePortNo = remPort.toString();
            queryPDU.payload.setQueryCursor(cursor);
            queryPDU.payload.setQueryResponder(nodePtr.chord_id);
            MessageParams nodeMsg = new MessageParams(
                    queryPDU.payload, remotePortNo, MESSAGE_TYPES.QUERY_RESP);
            new Thread(new ClientTask(nodeMsg)).start();
            cursor.close();
        }
    }

    private Cursor generateQueryAllResponse(String key) {
        MatrixCursor mCursor = new MatrixCursor(
                new String[] {
                        LocalDHTable.COLUMN_KEY,
                        LocalDHTable.COLUMN_VALUE
                });
        while (!queryAllResponseQueue.containsKey(key)) {

        }
        Map<String, DHTStore> qMap = queryAllResponseQueue.get(key);
        queryAllResponseQueue.remove(key);
        if (qMap != null) {
            Iterator<String> itr = qMap.keySet().iterator();
            String keyIter = null;
            DHTStore valIter = null;
            while (itr.hasNext()) {
                keyIter = itr.next();
                valIter = qMap.get(keyIter);
                mCursor.newRow().add(keyIter).add(valIter.getValue());
            }
        }
        return mCursor;
    }

    private Cursor generateQueryResponse(NodeInfo nodeInfo, String key, String reqId) {
        MatrixCursor mCursor = new MatrixCursor(
                new String[] {
                        LocalDHTable.COLUMN_KEY,
                        LocalDHTable.COLUMN_VALUE
                });
        while (!queryResponseQueue.containsKey(reqId + key)) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        Map<String, DHTStore> qMap = queryResponseQueue.remove(reqId + key);
        if (qMap != null) {
            Iterator<String> itr = qMap.keySet().iterator();
            String keyIter = null;
            DHTStore valIter = null;
            while (itr.hasNext()) {
                keyIter = itr.next();
                valIter = qMap.get(keyIter);
                mCursor.newRow().add(valIter.getKey()).add(valIter.getValue());
            }
        }
        return mCursor;
    }

    public ChordPDU startRecover(PayLoad payload) {
        synchronized (queryLock) {
            String queryStr = "select * from " + LocalDHTable.TABLE_DYNAMO;
            queryStr += " where node like '" + payload.getKey() + "'";
            SQLiteDatabase db = database.getWritableDatabase();
            Cursor cursor = db.rawQuery(queryStr, null);
            payload.setQueryCursor(cursor);
            ChordPDU chordPDU = new ChordPDU(MESSAGE_TYPES.RECOVER, payload);
            cursor.close();
            return chordPDU;
        }
    }

    public void sendDeletePDU(String portId, String selection, MESSAGE_TYPES mesgType) {
        Integer remPort = Integer.parseInt(portId) * 2;
        String remotePortNo = remPort.toString();
        PayLoad payload = new PayLoad();
        payload.setKey(selection);
        MessageParams nodeMsg = new MessageParams(payload, remotePortNo, mesgType);
        Log.d(TAG, nodeMsg.toString());
        new Thread(new ClientTask(nodeMsg)).start();
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        SQLiteDatabase db = database.getWritableDatabase();
        selection = selection.trim();
        if (selection.equalsIgnoreCase("@")) {
            int deleteCount = db.delete(LocalDHTable.TABLE_DYNAMO, null, null);
            getContext().getContentResolver().notifyChange(uri, null);
            return deleteCount;
        } else {
            try {
                if (selection.equalsIgnoreCase("*")) {
                    int deleteCount = db.delete(LocalDHTable.TABLE_DYNAMO, null, null);
                    NodeInfo queryAllNodes = nodePtr.dynamoRing.get(nodePtr.node_id);
                    Integer remPort = Integer.parseInt(queryAllNodes.getSuccessorChord1Id()) * 2;
                    String remotePortNo = remPort.toString();
                    PayLoad payload = new PayLoad();
                    payload.setKey(selection);
                    payload.setQueryInitiator(nodePtr.chord_id);
                    payload.setDeleteCount(deleteCount);
                    payload.setValue("KEY");
                    MessageParams nodeMsg = new MessageParams(payload, remotePortNo,
                            MESSAGE_TYPES.DELETE_ALL);
                    new Thread(new ClientTask(nodeMsg)).start();
                    try {
                        int retVal = deleteResponseQueue.take();
                        deleteResponseQueue.clear();
                        return retVal;
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                } else {
                    String key = genHash(selection);
                    NodeInfo nodeInfo = nodePtr.locateNodeInRing(key);
                    if (nodeInfo.getPortId().equals(nodePtr.chord_id)) {
                        PayLoad payload = new PayLoad();
                        payload.setKey(selection);
                        ChordPDU chordPDU = new ChordPDU(MESSAGE_TYPES.DELETE, payload);
                        deleteFromDHT(chordPDU);
                    } else {
                        sendDeletePDU(nodeInfo.getPortId(), selection, MESSAGE_TYPES.DELETE);
                    }
                    return 0;
                }
            } catch (NoSuchAlgorithmException nsae) {
                Log.e(TAG, nsae.toString());
            }
        }
        return 0;
    }

    public void deleteReplicaFromDHT(ChordPDU deletePDU) {
        synchronized (insertLock) {
            SQLiteDatabase db = database.getWritableDatabase();
            db.delete(LocalDHTable.TABLE_DYNAMO,
                    LocalDHTable.COLUMN_KEY + " = '" + deletePDU.payload.getKey() + "'",
                    null);
            concurrentMap.remove(deletePDU.payload.getKey());
        }
        if (deletePDU.messageType == MESSAGE_TYPES.DELETE_REPLICA1) {
            try {
                NodeInfo nodeInfo1 = nodePtr.locateNodeInRing(genHash(deletePDU.payload.getKey()));
                sendDeletePDU(nodeInfo1.getSuccessorChord2Id(), deletePDU.payload.getKey(),
                        MESSAGE_TYPES.DELETE_REPLICA2);
            } catch (NoSuchAlgorithmException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public void deleteFromDHT(ChordPDU deletePDU) {
        SQLiteDatabase db = database.getWritableDatabase();
        if (deletePDU.messageType == MESSAGE_TYPES.DELETE_ALL) {
            if (nodePtr.chord_id.equals(deletePDU.payload.getQueryInitiator())) {
                try {
                    Log.d("DSPOCK", "Query_all ring completed... Count > "
                            + deletePDU.payload.getQueryCursor().size());
                    deleteResponseQueue.put(deletePDU.payload.getDeleteCount());
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            } else {
                int deleteCount = db.delete(LocalDHTable.TABLE_DYNAMO, null, null);
                deleteCount += deletePDU.payload.getDeleteCount();
                deletePDU.payload.setDeleteCount(deleteCount);
                NodeInfo queryAllNodes = nodePtr.dynamoRing.get(nodePtr.node_id);
                Integer remPort = Integer.parseInt(queryAllNodes.getSuccessorChord1Id()) * 2;
                String remotePortNo = remPort.toString();
                MessageParams nodeMsg = new MessageParams(deletePDU.payload, remotePortNo,
                        MESSAGE_TYPES.DELETE_ALL);
                new Thread(new ClientTask(nodeMsg)).start();
            }
        } else {
            try {
                NodeInfo nodeInfo1 = nodePtr.locateNodeInRing(genHash(deletePDU.payload.getKey()));
                synchronized (insertLock) {
                    int deleteCount = db.delete(LocalDHTable.TABLE_DYNAMO,
                            LocalDHTable.COLUMN_KEY + " = '" + deletePDU.payload.getKey() + "'",
                            null);
                    deletePDU.payload.setDeleteCount(deleteCount);
                    concurrentMap.remove(deletePDU.payload.getKey());
                }
                Log.d(TAG,
                        "COORD_DELETE_REPLICA :: COORDINATOR for this key is : "
                                + nodeInfo1.getPortId()
                                + ", SUCC1 : " + nodeInfo1.getSuccessorChord1Id()
                                + ", SUCC2 : " + nodeInfo1.getSuccessorChord2Id());
                sendDeletePDU(nodeInfo1.getSuccessorChord1Id(), deletePDU.payload.getKey(),
                        MESSAGE_TYPES.DELETE_REPLICA1);
            } catch (NoSuchAlgorithmException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
            String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private static Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        // uriBuilder.appendPath("dht");
        return uriBuilder.build();
    }

    public void createServerSocket() {
        try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that
             * listens on the server port.
             */
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            Log.d(TAG,
                    "Server Socket Created :: " + serverSocket.getInetAddress()
                            + ":" + serverSocket.getLocalPort());
            new Thread(new ServerTask(serverSocket)).start();
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }
    }

    /**
     * CLient Task
     * 
     * @author dileepra
     */
    public class ClientTask implements Runnable {

        static final String TAG = "ClientTask";
        private MessageParams mParams;

        public ClientTask(MessageParams mParams) {
            this.mParams = mParams;
        }

        @Override
        public void run() {
            ChordPDU chordPDU = null;
            chordPDU = new ChordPDU(mParams.messageType, mParams.payload);
            try {
                sendMessage(chordPDU, mParams.remotePort);
            } catch (Exception e) {
                try {
                    // First fail
                    MESSAGE_TYPES replicaType = MESSAGE_TYPES.INSERT_RESP;
                    String key = genHash(chordPDU.payload.getKey());
                    NodeInfo nodeInfo = nodePtr.locateNodeInRing(key);
                    Integer remPort = Integer.parseInt(nodeInfo.getSuccessorChord1Id()) * 2;
                    String remotePortNo = remPort.toString();
                    if (mParams.messageType == MESSAGE_TYPES.INSERT_NOW) {
                        replicaType = MESSAGE_TYPES.INSERT_REPLICA1;
                    } else if (mParams.messageType == MESSAGE_TYPES.QUERY) {
                        replicaType = MESSAGE_TYPES.QUERY_REPLICA1;
                    } else if (mParams.messageType == MESSAGE_TYPES.QUERY_ALL) {
                        NodeInfo currentNodeInfo = nodePtr
                                .locateNodeInRing(genHash(nodePtr.chord_id));
                        remPort = Integer.parseInt(currentNodeInfo.getSuccessorChord2Id()) * 2;
                        remotePortNo = remPort.toString();
                        replicaType = MESSAGE_TYPES.QUERY_ALL;
                    } else if (mParams.messageType == MESSAGE_TYPES.DELETE) {
                        replicaType = MESSAGE_TYPES.DELETE_REPLICA1;
                    } else if (mParams.messageType == MESSAGE_TYPES.INSERT_REPLICA1) {
                        remPort = Integer.parseInt(nodeInfo.getSuccessorChord2Id()) * 2;
                        remotePortNo = remPort.toString();
                        replicaType = MESSAGE_TYPES.INSERT_REPLICA2;
                    } else if (mParams.messageType == MESSAGE_TYPES.QUERY_REPLICA1) {
                        remPort = Integer.parseInt(nodeInfo.getPortId()) * 2;
                        remotePortNo = remPort.toString();
                        replicaType = MESSAGE_TYPES.QUERY_REPLICA2;
                    } else if (mParams.messageType == MESSAGE_TYPES.DELETE_REPLICA1) {
                        remPort = Integer.parseInt(nodeInfo.getSuccessorChord2Id()) * 2;
                        remotePortNo = remPort.toString();
                        replicaType = MESSAGE_TYPES.DELETE_REPLICA2;
                    } else {
                        return;
                    }

                    chordPDU = new ChordPDU(replicaType, mParams.payload);
                    try {
                        sendMessage(chordPDU, remotePortNo);
                    } catch (Exception ex) {
                        // Second Fail
                        remPort = Integer.parseInt(nodeInfo.getSuccessorChord2Id()) * 2;
                        remotePortNo = remPort.toString();
                        if (replicaType == MESSAGE_TYPES.INSERT_REPLICA1) {
                            replicaType = MESSAGE_TYPES.INSERT_REPLICA2;
                        } else if (replicaType == MESSAGE_TYPES.QUERY_REPLICA1) {
                            remPort = Integer.parseInt(nodeInfo.getPortId()) * 2;
                            remotePortNo = remPort.toString();
                            replicaType = MESSAGE_TYPES.QUERY_REPLICA2;
                        } else if (replicaType == MESSAGE_TYPES.DELETE_REPLICA1) {
                            replicaType = MESSAGE_TYPES.DELETE_REPLICA2;
                        } else if (mParams.messageType == MESSAGE_TYPES.QUERY_ALL) {
                            NodeInfo currentNodeInfo = nodePtr
                                    .locateNodeInRing(genHash(nodePtr.chord_id));
                            String nextTarget = currentNodeInfo.getSuccessorChord2Id();
                            currentNodeInfo = nodePtr.locateNodeInRing(genHash(nextTarget));
                            remPort = Integer.parseInt(currentNodeInfo.getSuccessorChord1Id()) * 2;
                            remotePortNo = remPort.toString();
                            replicaType = MESSAGE_TYPES.QUERY_ALL;
                        } else {
                            return;
                        }
                        chordPDU = new ChordPDU(replicaType, mParams.payload);
                        try {
                            sendMessage(chordPDU, remotePortNo);
                        } catch (Exception ex2) {
                            if (replicaType == MESSAGE_TYPES.QUERY_REPLICA2) {
                                Map<String, DHTStore> nullMap = new HashMap<String, DHTStore>();
                                queryResponseQueue.put(chordPDU.payload.getReqId()
                                        + chordPDU.payload.getKey(), nullMap);
                            }
                            return;
                        }
                    }
                } catch (NoSuchAlgorithmException nose) {
                    nose.printStackTrace();
                }
            }
            return;
        }

        private void sendMessage(ChordPDU message, String remotePort) throws Exception {
            Socket socket = null;
            // int timeout = 5000;
            boolean success = false;
            ObjectOutputStream clientOut = null;
            BufferedReader clientIn = null;
            try {
                socket = new Socket();
                socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[] {
                        10, 0, 2, 2
                }),
                        Integer.parseInt(remotePort)), 1000);
                // socket.setSoTimeout(1000);
                // socket.setKeepAlive(true);
                Log.d(TAG, "Inside Client Task :: PDU=" + message.messageType.toString()
                        + ", Request To :: "
                        + Integer.parseInt(remotePort) / 2 + ", From :: "
                        + nodePtr.chord_id);

                /*
                 * TODO: Fill in your client code that sends out a message.
                 */
                BufferedReader inStream = new BufferedReader(new InputStreamReader(
                        socket.getInputStream()));
                String responseServer = inStream.readLine();
                // socket.setSoTimeout(0);
                if (responseServer == null || responseServer.trim().length() == 0
                        || !responseServer.equals("OK")) {
                    success = false;
                } else {
                    // Log.e(TAG, responseServer);
                    success = true;
                }
                if (success && socket.isConnected()) {
                    clientOut = new ObjectOutputStream(
                            new BufferedOutputStream(socket.getOutputStream()));
                    clientOut.writeObject(message);
                    clientOut.flush();
                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
                success = false;
            } catch (IOException e) {
                Log.e(TAG, "Inside ClientTask SendMessage ::: -->" + e.toString());
                e.printStackTrace();
                success = false;
            } catch (Exception ex) {
                Log.e(TAG, ex.toString());
                success = false;
            } finally {
                if (null != socket) {
                    socket.close();
                    try {
                        if (clientOut != null) {
                            clientOut.flush();
                            clientOut.close();
                        }
                        if (clientIn != null) {
                            clientIn.close();
                        }
                    } catch (IOException e) {
                        Log.e(TAG, "Exception occured while closing Client Socket");
                    }
                }
                if (!success) {
                    throw new Exception();
                }
            }
        }

    }

    /***
     * ServerTask for processing client request @author dileepra
     */
    public class ServerTask implements Runnable {

        static final String TAG = "ServerTask";
        private ServerSocket serverSocket;

        public ServerTask(ServerSocket socket) {
            this.serverSocket = socket;
        }

        @Override
        public void run() {
            /*
             * TODO: Fill in your server code that receives messages and passes
             * them to onProgressUpdate().
             */
            Log.d(TAG, "Server waiting on " + serverSocket.getLocalPort());
            ChordPDU tempMsg = null;
            while (true) {
                Socket tempSocket = null;
                try {
                    tempSocket = serverSocket.accept();
                    Log.d(TAG,
                            "Server Connected to client port "
                                    + tempSocket.getRemoteSocketAddress());
                    new Thread(new ServerSubTask(tempSocket)).start();
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    break;
                }
            }
        }
    }

    public class ServerSubTask implements Runnable {

        static final String TAG = "ServerTask";
        private Socket tempSocket;

        public ServerSubTask(Socket socket) {
            this.tempSocket = socket;
        }

        @Override
        public void run() {
            try {
                PrintWriter bufOut = new PrintWriter(tempSocket.getOutputStream(), true);
                bufOut.println("OK");
                bufOut.flush();
                ObjectInputStream in = new ObjectInputStream(
                        new BufferedInputStream(tempSocket.getInputStream()));
                ChordPDU tempMsg = (ChordPDU) in.readObject();
                MessageParams mParams = new MessageParams(tempMsg.payload, tempSocket,
                        tempMsg.messageType);

                switch (tempMsg.messageType) {
                    case INSERT_NOW:
                        ContentValues cv1 = new ContentValues();
                        cv1.put(LocalDHTable.COLUMN_KEY, tempMsg.payload.getKey());
                        cv1.put(LocalDHTable.COLUMN_VALUE, tempMsg.payload.getValue());
                        insertToDHT(mUri, cv1);
                        break;
                    case INSERT_REPLICA1:
                    case INSERT_REPLICA2: // Failure
                        ContentValues cv2 = new ContentValues();
                        cv2.put(LocalDHTable.COLUMN_KEY, tempMsg.payload.getKey());
                        cv2.put(LocalDHTable.COLUMN_VALUE, tempMsg.payload.getValue());
                        insertReplicaToDHT(mUri, cv2, tempMsg.messageType);
                        break;
                    case QUERY_ALL:
                    case QUERY_REPLICA1:
                    case QUERY_REPLICA2:
                    case QUERY:
                        queryDHT(tempMsg);
                        break;
                    case QUERY_RESP:
                        queryResponseQueue.put(tempMsg.payload.getReqId()
                                + tempMsg.payload.getKey(),
                                tempMsg.payload.getQueryCursor());
                        break;
                    case DELETE_ALL:
                    case DELETE:
                        deleteFromDHT(tempMsg);
                        break;
                    case DELETE_REPLICA1:
                    case DELETE_REPLICA2:
                        deleteReplicaFromDHT(tempMsg);
                        break;
                    case DELETE_RESP:
                        deleteResponseQueue.put(tempMsg.payload.getDeleteCount());
                        break;
                    case RECOVER:
                        ChordPDU recoverPDU = startRecover(tempMsg.payload);
                        ObjectOutputStream clientOut = new ObjectOutputStream(
                                new BufferedOutputStream(tempSocket.getOutputStream()));
                        clientOut.writeObject(recoverPDU);
                        clientOut.flush();
                        break;
                    default:
                        break;
                }
            } catch (IOException e) {
                Log.e(TAG, "IOException ::: " + e.toString());
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } finally {
                if (null != tempSocket) {
                    try {
                        tempSocket.close();
                    } catch (IOException e) {
                        Log.e(TAG,
                                "Exception occured while closing Server Socket");
                    }
                }
            }

        }
    }

    /**
     * Process Recovery Asynctask
     * 
     * @author dileepra
     */
    public class RecoveryTask implements Runnable {
        private MessageParams mParams;
        static final String TAG = "RecoveryTask";

        public RecoveryTask(MessageParams mParams) {
            this.mParams = mParams;
        }

        public void insertRecover(PayLoad payload) {
            Log.e(TAG, "RECOVERY insertion");
            SQLiteDatabase sqlDB = database.getWritableDatabase();
            synchronized (insertLock) {
                try {
                    Map<String, DHTStore> recMap = payload.getQueryCursor();
                    Set<String> recSet = recMap.keySet();
                    for (String recKey : recSet) {
                        DHTStore dhtStore = recMap.get(recKey);
                        ContentValues cv = new ContentValues();
                        cv.put(LocalDHTable.COLUMN_KEY, dhtStore.getKey());
                        cv.put(LocalDHTable.COLUMN_VALUE, dhtStore.getValue());
                        cv.put(LocalDHTable.COLUMN_VERSION, dhtStore.getVersion());
                        cv.put(LocalDHTable.COLUMN_NODE, dhtStore.getNode());
                        sqlDB.replace(LocalDHTable.TABLE_DYNAMO, null, cv);
                        concurrentMap.put(dhtStore.getKey(),
                                Integer.parseInt(dhtStore.getVersion()));
                    }
                } catch (SQLException e) {
                } finally {
                }
            }
            Log.e(TAG, "RECOVERY COMPLETE for node:" + nodePtr.chord_id);
        }

        @Override
        public void run() {
            ChordPDU chordPDU = null;
            chordPDU = new ChordPDU(mParams.messageType, mParams.payload);
            sendMessage(chordPDU, mParams.remotePort);
        }

        private void sendMessage(ChordPDU message, String remotePort) {
            Socket socket = null;
            ObjectOutputStream clientOut = null;
            ObjectInputStream clientIn = null;
            try {
                socket = new Socket(
                        InetAddress.getByAddress(new byte[] {
                                10, 0, 2, 2
                        }),
                        Integer.parseInt(remotePort));
                Log.d(TAG, "Inside Recovery Task :: PDU=" + message.messageType.toString()
                        + ", Request To :: "
                        + Integer.parseInt(remotePort) / 2 + ", From :: "
                        + nodePtr.chord_id);

                /*
                 * TODO: Fill in your client code that sends out a message.
                 */
                BufferedReader inStream = new BufferedReader(new InputStreamReader(
                        socket.getInputStream()));
                String responseServer = inStream.readLine();
                // socket.setSoTimeout(0);
                if (responseServer != null && responseServer.equals("OK")) {
                    if (socket.isConnected()) {
                        Log.i(TAG, "CONNECTED TO RECOVERY SERVER!!!");
                        clientOut = new ObjectOutputStream(
                                new BufferedOutputStream(socket.getOutputStream()));
                        clientOut.writeObject(message);
                        clientOut.flush();
                        clientIn = new ObjectInputStream(new BufferedInputStream(
                                socket.getInputStream()));
                        ChordPDU recoveryPDU = (ChordPDU) clientIn.readObject();
                        insertRecover(recoveryPDU.payload);
                    }
                } else {
                    Log.i(TAG, "FAILED TO CONNECT TO RECOVERY!!!");
                }

            } catch (UnknownHostException e) {
                Log.i(TAG, "ClientTask UnknownHostException");
                Log.i(TAG, "FAILED TO CONNECT TO RECOVERY!!!");
            } catch (IOException e) {
                e.printStackTrace();
                Log.i(TAG, "FAILED TO CONNECT TO RECOVERY!!!");
            } catch (Exception ex) {
                Log.i(TAG, ex.toString());
                Log.i(TAG, "FAILED TO CONNECT TO RECOVERY!!!");
            } finally {
                if (null != socket) {
                    try {
                        if (clientOut != null) {
                            clientOut.close();
                        }
                        if (clientIn != null) {
                            clientIn.close();
                        }
                        socket.close();
                    } catch (IOException e) {
                        Log.e(TAG, "Exception occured while closing Client Socket");
                    }
                }
            }
        }
    }
}
