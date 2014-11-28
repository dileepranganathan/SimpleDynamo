/**
 * @author Dileep Ranganathan
 */
package edu.buffalo.cse.cse486586.simpledynamo;

import android.database.Cursor;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

public class PayLoad implements Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = -6388142463282455968L;
    private Map<String, DHTStore> queryCursor;
    private String reqId;
    private String key;
    private String value;
    private String queryInitiator;
    private String queryResponder;
    private Integer version;
    int deleteCount = 0;

    public Map<String, DHTStore> getQueryCursor() {
        return queryCursor;
    }

    public void setQueryCursor(Map<String, DHTStore> queryCursor) {
        this.queryCursor = queryCursor;
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

    public int getDeleteCount() {
        return deleteCount;
    }

    public void setDeleteCount(int deleteCount) {
        this.deleteCount = deleteCount;
    }

    public void setQueryCursor(Cursor cursor) {
        queryCursor = new LinkedHashMap<String, DHTStore>();
        mergeCursor(cursor);
    }

    public void mergeCursor(Cursor cursor) {
        if (cursor == null || cursor.getCount() < 1) {
            return;
        }
        if (cursor.moveToFirst()) {
            // Log.d("DSPOCK", "Query_All Inside setQueryCursor");
            while (!cursor.isAfterLast()) {
                int keyIndex = cursor.getColumnIndex(LocalDHTable.COLUMN_KEY);
                int valueIndex = cursor
                        .getColumnIndex(LocalDHTable.COLUMN_VALUE);
                int versionIndex = cursor.getColumnIndex(LocalDHTable.COLUMN_VERSION);
                int nodeIndex = cursor.getColumnIndex(LocalDHTable.COLUMN_NODE);
                String returnKey = cursor.getString(keyIndex);
                String returnValue = cursor.getString(valueIndex);
                String returnVersion = cursor.getString(versionIndex);
                String returnNode = cursor.getString(nodeIndex);
                /*
                 * Log.d("DSPOCK", "Query_All Inside setQueryCursor > key=" +
                 * returnKey + ", value=" + returnValue);
                 */
                DHTStore dhtStore = new DHTStore(returnKey, returnValue, returnVersion, returnNode);
                queryCursor.put(returnKey, dhtStore);
                cursor.moveToNext();
            }
        }
    }

    public String getQueryInitiator() {
        return queryInitiator;
    }

    public void setQueryInitiator(String queryInitiator) {
        this.queryInitiator = queryInitiator;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public String getQueryResponder() {
        return queryResponder;
    }

    public void setQueryResponder(String queryResponder) {
        this.queryResponder = queryResponder;
    }

    public String getReqId() {
        return reqId;
    }

    public void setReqId(String reqId) {
        this.reqId = reqId;
    }
}
