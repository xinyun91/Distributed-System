package edu.buffalo.cse.cse486586.simpledht;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.apache.http.util.EncodingUtils;

public class SimpleDhtProvider extends ContentProvider {
    static final int SERVER_PORT = 10000;
    String portStr;
    String myPort;
    String pre;
    String suc;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        try{
            if(selection.equals("*")) {
                String node = portStr;
                while(true){
                    Socket inSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(node)*2);
                    DataOutputStream out = new DataOutputStream(inSocket.getOutputStream());
                    out.writeUTF("globalDelete");
                    DataInputStream in = new DataInputStream(inSocket.getInputStream());
                    node = in.readUTF();
                    if(node.equals(portStr)){
                        break;
                    }
                    inSocket.close();
                }
            } else if(selection.equals("@")){
                String globalList[] = this.getContext().fileList();
                for(String _perKey: globalList) {
                    this.getContext().deleteFile(_perKey);
                }
            } else {
                String target = find(selection);
                Socket inSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(target)*2);
                DataOutputStream out = new DataOutputStream(inSocket.getOutputStream());
                out.writeUTF("delete,"+selection);
                out.flush();
                out.close();
            }
        }catch(Exception e){
            Log.e("delete", e.toString());
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        try {
            String fileName = values.getAsString("key");
            String content = values.getAsString("value");
            String target = find(fileName);
            Socket inSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(target)*2);
            DataOutputStream out = new DataOutputStream(inSocket.getOutputStream());
            out.writeUTF("insert,"+fileName+","+content);
            out.flush();
            out.close();
        } catch(Exception e) {
            Log.e("insertSend", e.toString());
        }
        Log.v("insert", values.toString());
        return uri;
    }

    @Override
    public boolean onCreate() {
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
            portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            myPort = String.valueOf((Integer.parseInt(portStr) * 2));
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
            return true;
        }catch(Exception e){
            Log.e("socket",e.toString());
            return false;
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        String[] table = new String[] {"key","value"};
        MatrixCursor cursor = new MatrixCursor(table);
        try{
            if (selection.equals("*")) {
                /* add global data to cursor and return */
                String node = portStr;
                while(true){
                    Socket inSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(node)*2);
                    DataOutputStream out = new DataOutputStream(inSocket.getOutputStream());
                    out.writeUTF("globalQuery");
                    DataInputStream in = new DataInputStream(inSocket.getInputStream());
                    String flow;
                    while((flow = in.readUTF()) != null) {
                        String content[] = flow.split(",");
                        if (content.length == 2) {
                            cursor.addRow(new Object[]{content[0], content[1]});
                        } else if (content.length == 1) {
                            node = content[0];
                            if (content[0].equals(portStr)) {
                                return cursor;
                            }
                            inSocket.close();
                            break;
                        }
                    }
                }
            } else if(selection.equals("@")){
                /* add local data to cursor and return */
                String globalList[] = this.getContext().fileList();
                for(String _perKey: globalList) {
                    FileInputStream q = getContext().openFileInput(_perKey);
                    int l = q.available();
                    byte[] reads = new byte[l];
                    q.read(reads);
                    String perVal = EncodingUtils.getString(reads, "UTF-8");
                    cursor.addRow(new Object[]{_perKey, perVal});
                }
            } else {
                /* get data by selection */
                /* convert byte to string */
                /* add the queried data to cursor and return*/
                String target = find(selection);
                Socket inSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(target)*2);
                DataOutputStream out = new DataOutputStream(inSocket.getOutputStream());
                out.writeUTF("query,"+selection);
                out.flush();
                DataInputStream in = new DataInputStream(inSocket.getInputStream());
                String content = in.readUTF();
                cursor.addRow(new Object[]{selection, content});
                inSocket.close();
            }
        }catch(Exception e){
            Log.e("queryError",e.toString());
        }
        Log.v("query", selection);
        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
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

    private String find(String key) {
        try {
            String one = portStr;
            while(true){
                if(pre==suc) return one;
                Socket inSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(one) * 2);
                DataOutputStream out = new DataOutputStream(inSocket.getOutputStream());
                out.writeUTF("find," + key);
                DataInputStream in = new DataInputStream(inSocket.getInputStream());
                String result[] = in.readUTF().split(",");
                String attribute = result[0];
                if(attribute.equals("found")){
                    inSocket.close();
                    return result[1];
                }else if(attribute.equals("notfound")){
                    one = result[1];
                    inSocket.close();
                }
            }
        } catch (Exception e) {

        }
        return null;
    }

    private Void join(String portStr, String last, String next, String key) {
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, portStr, last, next, key);
        return null;
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

            Socket newSocket = null;
            try {
                while(true) {
                    newSocket = serverSocket.accept();
                    DataInputStream in = new DataInputStream(newSocket.getInputStream());
                    String req ;
                    if((req = in.readUTF()) != null){
                        String request[] = req.split(",");
                        String attribute = request[0];
                        if(attribute.equals("find")) {
                            String key = request[1];
                            if((genHash(key).compareTo(genHash(portStr)) < 0 && genHash(key).compareTo(genHash(pre))>0) ||
                                    (genHash(portStr).compareTo(genHash(pre))<0 && (genHash(key).compareTo(genHash(pre))>0 || genHash(key).compareTo(genHash(portStr))<0))) {
                                DataOutputStream out = new DataOutputStream(newSocket.getOutputStream());
                                out.writeUTF("found,"+portStr);
                                out.flush();
                            } else {
                                DataOutputStream out = new DataOutputStream(newSocket.getOutputStream());
                                out.writeUTF("notfound,"+suc);
                                out.flush();
                            }
                        }else if(attribute.equals("insert")) {
                            String key = request[1];
                            String values = request[2];
                            FileOutputStream save = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                            save.write(values.getBytes());
                            save.close();
                        }else if(attribute.equals("query")) {
                            String key = request[1];
                            FileInputStream q = getContext().openFileInput(key);
                            int l = q.available();
                            byte[] reads = new byte[l];
                            q.read(reads);
                            String content = EncodingUtils.getString(reads, "UTF-8");
                            DataOutputStream out = new DataOutputStream(newSocket.getOutputStream());
                            out.writeUTF(content);
                            out.flush();
                        }else if(attribute.equals("join")){
                            String newPort = request[1];
                            if ((genHash(newPort).compareTo(genHash(portStr)) > 0 &&
                                    genHash(newPort).compareTo(genHash(suc)) < 0) || (pre.equals(suc) && portStr.equals(pre)) ||
                                    (genHash(portStr).compareTo(genHash(suc))>0 && (genHash(newPort).compareTo(genHash(portStr)) > 0 ||
                                            genHash(newPort).compareTo(genHash(suc)) < 0))) {
                                DataOutputStream out = new DataOutputStream(newSocket.getOutputStream());
                                if(genHash(newPort).compareTo(myPort)>0) {
                                    out.writeUTF(portStr + "," + suc + ",updatePre");
                                    suc = newPort;
                                } else {
                                    out.writeUTF(pre + "," + portStr + ",updateSuc");
                                    pre = newPort;
                                }
                                out.flush();
                            } else {
                                DataOutputStream out = new DataOutputStream(newSocket.getOutputStream());
                                out.writeUTF(suc);
                                out.flush();
                            }
                        } else if(attribute.equals("updatePre")){
                            String key = request[1];
                            pre = key;
                        } else if(attribute.equals("updateSuc")){
                            String key = request[1];
                            suc = key;
                        } else if(attribute.equals("delete")){
                            getContext().deleteFile(request[1]);
                        } else if(attribute.equals("globalQuery")){
                            String keys[] = getContext().fileList();
                            DataOutputStream out = new DataOutputStream(newSocket.getOutputStream());
                            for(String per: keys){
                                FileInputStream q = getContext().openFileInput(per);
                                int l = q.available();
                                byte[] reads = new byte[l];
                                q.read(reads);
                                String content = EncodingUtils.getString(reads, "UTF-8");
                                out.writeUTF(per + "," + content);
                            }
                            out.writeUTF(suc);
                        } else if(attribute.equals("globalDelete")){
                            String keys[] = getContext().fileList();
                            for(String per: keys) {
                                getContext().deleteFile(per);
                            }
                            DataOutputStream out = new DataOutputStream(newSocket.getOutputStream());
                            out.writeUTF(suc);
                        }
                    }
                }
            } catch(Exception e) {
                Log.e("genHash", e.toString());
            }
            return null;
        }

        protected void onProgressUpdate(String...strings) {
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
            try {
                pre = portStr;
                suc = portStr;
                if (!portStr.equals("5554")) {
                    String master = "5554";
                    boolean join = true;
                    while (join) {
                        Socket socket = new Socket();
                        socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(master) * 2), 5000);
                        if (socket.isClosed()) {
                            return null;
                        }
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        out.writeUTF("join," + portStr);
                        out.flush();
                        DataInputStream in = new DataInputStream(socket.getInputStream());
                        String read = in.readUTF();
                        String newNode[] = read.split(",");
                        if (newNode.length == 3) {
                            pre = newNode[0];
                            suc = newNode[1];
                            Socket updateSocket;
                            updateSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(suc) * 2);
                            DataOutputStream update = new DataOutputStream(updateSocket.getOutputStream());
                            update.writeUTF(newNode[2] + "," + portStr);
                            update.flush();
                            updateSocket.close();
                            join = false;
                        } else {
                            String nnext = newNode[0];
                            master = nnext;
                        }
                        socket.close();
                        Log.e("update", portStr + pre + suc);
                    }
                }
            } catch(Exception e){

            }
            return null;
        }
    }
}
