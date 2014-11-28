/**
 * @author Dileep Ranganathan
 */
package edu.buffalo.cse.cse486586.simpledynamo;

import android.database.sqlite.SQLiteDatabase;
import android.util.Log;

public class LocalDHTable {

    // Database table
    public static final String TABLE_DYNAMO = "dynamotable";
    public static final String COLUMN_KEY = "key";
    public static final String COLUMN_VALUE = "value";
    public static final String COLUMN_VERSION = "version";
    public static final String COLUMN_NODE = "node";

    // Database creation SQL statement
    private static final String DATABASE_CREATE = "create table "
            + TABLE_DYNAMO
            + "("
            + COLUMN_KEY + " TEXT PRIMARY KEY, "
            + COLUMN_VALUE + " TEXT, "
            + COLUMN_VERSION + " TEXT, "
            + COLUMN_NODE + " TEXT"
            + ");";

    public static void onCreate(SQLiteDatabase database) {
        database.execSQL(DATABASE_CREATE);
    }

    public static void onUpgrade(SQLiteDatabase database, int oldVersion,
            int newVersion) {
        Log.w(LocalDHTable.class.getName(), "Upgrading database from version "
                + oldVersion + " to " + newVersion
                + ", which will destroy all old data");
        database.execSQL("DROP TABLE IF EXISTS " + TABLE_DYNAMO);
        onCreate(database);
    }
}
