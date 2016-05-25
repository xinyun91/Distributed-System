package edu.buffalo.cse.cse486586.groupmessenger1;

import android.app.Activity;
import android.content.ContentValues;
import android.content.ContentResolver;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {
    static final int SERVER_PORT = 10000;
    public Uri uri;
    int count = 0;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        setContentView(R.layout.activity_group_messenger);

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */

        final TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        }catch(Exception e){
            Log.e("socket",e.toString());
            return;
        }

        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));
        
        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
        final EditText editText = (EditText) findViewById(R.id.editText1);
        findViewById(R.id.button4).setOnClickListener(new View.OnClickListener() {
            public void onClick(View t) {
                /*used in PA1; get text from */
                String msg = editText.getText().toString() + "\n";
                editText.setText("");
                tv.append("\t" + msg);
                //count++;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, "11108", "11112", "11116", "11120", "11124");
                return;
            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    /***
     * ServerTask is an AsyncTask that should handle incoming messages. It is created by
     * ServerTask.executeOnExecutor() call in SimpleMessengerActivity.
     *
     * Please make sure you understand how AsyncTask works by reading
     * http://developer.android.com/reference/android/os/AsyncTask.html
     *
     * @author stevko
     *
     */
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
            try {
                while(true) {
                    Socket newSocket = serverSocket.accept();
                    DataInputStream in = new DataInputStream(newSocket.getInputStream());
                    String send = null;
                    if((send = in.readUTF())!=null){
                        publishProgress(send);
                        newSocket.close();
                    }
                }
            }catch (IOException e){
                Log.e("serverBackground", e.toString());
            }
            return null;
        }

        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();
            TextView t = (TextView) findViewById(R.id.textView1);
            t.append(strReceived +"\t\n");
            /*
                     * TODO: msg sequence
                     */
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority("edu.buffalo.cse.cse486586.groupmessenger1.provider");
            uriBuilder.scheme("content");
            uri =  uriBuilder.build();
            if(strReceived!="") {
                try {
                    ContentValues keyValueToInsert = new ContentValues();
                    keyValueToInsert.put("key", Integer.toString(count++));
                    keyValueToInsert.put("value", strReceived);
                    Uri u = getContentResolver().insert(uri, keyValueToInsert);
                } catch (Exception e) {
                    Log.e("zlerror", e.toString());
                }
            }


            /*
             * The following code creates a file in the AVD's internal storage and stores a file.
             *
             * For more information on file I/O on Android, please take a look at
             * http://developer.android.com/training/basics/data-storage/files.html
             */
            String filename = "GroupMessengerOutput";
            String string = strReceived + "\n";
            FileOutputStream outputStream;
            try {
                outputStream = openFileOutput(filename, Context.MODE_PRIVATE);
                outputStream.write(string.getBytes());
                outputStream.close();
            } catch (Exception e) {
                Log.e("serverStorage", "File write failed");
            }
            return;
        }
    }

    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author stevko
     *
     */

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            //int count = 0;
            try {
                for(int i=1; i<=5; i++){
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(msgs[i]));
                    String msgToSend = msgs[0];

                /*
                 * TODO: Fill in your client code that sends out a message.
                 */
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    out.writeUTF(msgToSend);
                    out.flush();
                    socket.close();
                }
            } catch (UnknownHostException e) {
                Log.e("client", "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e("client", "ClientTask socket IOException");
            }
            return null;
        }
    }
}
