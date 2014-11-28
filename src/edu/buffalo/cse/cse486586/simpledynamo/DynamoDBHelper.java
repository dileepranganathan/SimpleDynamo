/**
 * @author Dileep Ranganathan
 */
package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

public class DynamoDBHelper extends SQLiteOpenHelper {

    static final String DATABASE_NAME = "dynamo.db";
    private static final int DATABASE_VERSION = 1;

    public DynamoDBHelper(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }

    // Method is called during creation of the database
    @Override
    public void onCreate(SQLiteDatabase database) {
        LocalDHTable.onCreate(database);
    }

    // Method is called during an upgrade of the database,
    // e.g. if you increase the database version
    @Override
    public void onUpgrade(SQLiteDatabase database, int oldVersion,
            int newVersion) {
        LocalDHTable.onUpgrade(database, oldVersion, newVersion);
    }

}
