/**
 * @author Dileep Ranganathan
 */
package edu.buffalo.cse.cse486586.simpledynamo;

import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.EditText;
import android.widget.TextView;

public class SimpleDynamoActivity extends Activity {
    static final String TAG = "SimpleDynamoActivity";
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    private ContentResolver mContentResolver;
    private final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
    private ContentValues mContentValues;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dynamo);

        final TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        final EditText editText = (EditText) findViewById(R.id.editText1);
        findViewById(R.id.button4).setOnClickListener(new OnClickListener() {

            @Override
            public void onClick(View v) {
                String msg = editText.getText().toString() + "\n";
                editText.setText("");
                mContentValues = new ContentValues();
                mContentValues.put(KEY_FIELD, msg);
                mContentValues.put(VALUE_FIELD, msg);
                mContentResolver = getContentResolver();
                mContentResolver.insert(mUri, mContentValues);
                tv.append(msg + "\n");
            }
        });

        findViewById(R.id.ldump).setOnClickListener(new OnClickListener() {

            @Override
            public void onClick(View v) {
                mContentResolver = getContentResolver();
                try {
                    String msg = editText.getText().toString();
                    if (msg == null || msg.trim().equals("")) {
                        msg = "@";
                    }
                    editText.setText("");
                    Cursor resultCursor = mContentResolver.query(mUri, null,
                            msg, null, null);
                    if (resultCursor == null) {
                        Log.e(TAG, "Result null");

                        throw new Exception();

                    }
                    tv.setText("");
                    tv.append("\n###########LDUMP############\n");

                    if (resultCursor.moveToFirst()) {
                        // Log.d("DSPOCK", "Query_All Inside setQueryCursor");
                        while (!resultCursor.isAfterLast()) {
                            int keyIndex = resultCursor.getColumnIndex(LocalDHTable.COLUMN_KEY);
                            String returnKey = resultCursor.getString(keyIndex);
                            tv.append("\t" + returnKey + "\n");
                            resultCursor.moveToNext();
                        }
                    }

                    resultCursor.close();
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        });

        findViewById(R.id.delete).setOnClickListener(new OnClickListener() {

            @Override
            public void onClick(View v) {
                mContentResolver = getContentResolver();
                try {
                    String msg = editText.getText().toString();
                    if (msg == null || msg.trim().equals("")) {
                        msg = "@";
                    }
                    editText.setText("");
                    mContentResolver.delete(mUri, msg, null);
                    tv.setText("");
                    tv.append("\n###########Deleted############\n");
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.simple_dynamo, menu);
        return true;
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        // uriBuilder.appendPath("dht");
        return uriBuilder.build();
    }

}
