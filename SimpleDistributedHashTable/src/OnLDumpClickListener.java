package edu.buffalo.cse.cse486586.simpledht;


import android.content.ContentResolver;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.TextView;

/**
 * Created by xinyun91 on 3/26/16.
 */
public class OnLDumpClickListener implements OnClickListener {

    private final TextView mTV;
    private final ContentResolver mCR;
    private final Uri mUri;

    public OnLDumpClickListener(TextView _tv, ContentResolver _cr) {
        mTV = _tv;
        mCR = _cr;
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority("edu.buffalo.cse.cse486586.simpledht.provider");
        uriBuilder.scheme("content");
        mUri = uriBuilder.build();
    }

    @Override
    public void onClick(View v) {
        new Task().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
    }

    private class Task extends AsyncTask<Void, String, Void> {
        @Override
        protected Void doInBackground(Void... params) {
            Cursor c = mCR.query(mUri, null, "@", null, null);
            while(c.moveToNext()) {
                String perKey = c.getString(c.getColumnIndex("key"));
                String perVal = c.getString(c.getColumnIndex("value"));
                publishProgress(perKey + perVal + "\n");
            }
            c.close();
            mCR.delete(mUri, "@", null);
            return null;
        }
        protected void onProgressUpdate(String...strings) {
            mTV.append(strings[0]);
            return;
        }
    }

}
