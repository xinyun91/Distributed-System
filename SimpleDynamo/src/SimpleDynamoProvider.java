package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Formatter;
import java.util.Hashtable;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeoutException;

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

public class SimpleDynamoProvider extends ContentProvider {
	static final int SERVER_PORT = 10000;
	String portStr;
	String myPort;
	String nodes[] = {"5562","5556","5554","5558","5560"};

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		if(selection.equals("*")) {
			for(int i=0; i<nodes.length; i++){
				try {
					Socket inSocket = new Socket();
					inSocket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(nodes[i % 5]) * 2), 60000);
					DataOutputStream out = new DataOutputStream(inSocket.getOutputStream());
					out.writeUTF("globalDelete");
					inSocket.close();
				}catch(Exception e){
					Log.e("DeleteGlobalException", e.toString());
				}
			}
		}else if(selection.equals("@")){
			String globalList[] = this.getContext().fileList();
			for(String _perKey: globalList) {
				String coordinator = find(_perKey);
				int i = Arrays.asList(nodes).indexOf(coordinator);
				if(coordinator.equals(portStr)) {
					this.getContext().deleteFile(_perKey);
					for(int j=i+1;j<i+3;j++){
						try {
							Socket inSocket = new Socket();
							inSocket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(nodes[j % 5]) * 2),60000);
							DataOutputStream out = new DataOutputStream(inSocket.getOutputStream());
							out.writeUTF("delete," + _perKey);
							out.flush();
							out.close();
						}catch(Exception e){
							Log.e("DeleteLocalException", e.toString());
						}
					}
				}
			}
		}else {
			String coordinator = find(selection);
			int i = Arrays.asList(nodes).indexOf(coordinator);
			for(int j=i; j<=i+2; j++) {
				int index = j % 5;
				try {
					Socket inSocket = new Socket();
					inSocket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(nodes[index]) * 2),1000);
					DataOutputStream out = new DataOutputStream(inSocket.getOutputStream());
					out.writeUTF("delete," + selection);
					out.flush();
					out.close();
				}catch(Exception e){
					Log.e("DeletePerKeyException", e.toString());
				}
			}
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
		//new InsertTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, values);
		// TODO Auto-generated method stub
		String fileName = values.getAsString("key");
		String content = values.getAsString("value");
		String coordinator = find(fileName);
		int i = Arrays.asList(nodes).indexOf(coordinator);
		for(int j=i; j<=i+2; j++) {
			try {
				//Socket inSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
				//		Integer.parseInt(nodes[j%5]) * 2);
				//inSocket.setSoTimeout(1000);
				Socket inSocket = new Socket();
				inSocket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(nodes[j % 5]) * 2), 5000);
				DataOutputStream out = new DataOutputStream(inSocket.getOutputStream());
				out.writeUTF("insert," + fileName + "," + content);
				out.flush();
				out.close();
				//inSocket.close();
				//Log.v("insert","in the "+nodes[j%5]+ " " +values.toString());
			}catch(Exception e){
				Log.e("insertError", e.toString());
			}
		}
		/*final ContentValues vals = values;
		Thread t = new Thread(new Runnable() {
			@Override
			public void run() {
				String fileName = vals.getAsString("key");
				String content = vals.getAsString("value");
				String coordinator = find(fileName);
				int i = Arrays.asList(nodes).indexOf(coordinator);
				for(int j=i; j<=i+2; j++) {
					try {
						//Socket inSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						//		Integer.parseInt(nodes[j%5]) * 2);
						//inSocket.setSoTimeout(1000);
						Socket inSocket = new Socket();
						inSocket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(nodes[j % 5]) * 2), 5000);
						DataOutputStream out = new DataOutputStream(inSocket.getOutputStream());
						out.writeUTF("insert," + fileName + "," + content);
						out.flush();
						out.close();
						inSocket.close();
						//Log.v("insert","in the "+nodes[j%5]+ " " +values.toString());
					}catch(Exception e){
						Log.e("insertError", e.toString());
					}
				}
			}
		});
		t.start();
		try {
			t.join();
		}catch(Exception e){
			Log.e("InsertNotFinish", e.toString());
		}*/
		/*String fileName = values.getAsString("key");
		String content = values.getAsString("value");
		String coordinator = find(fileName);
		int i = Arrays.asList(nodes).indexOf(coordinator);
		for(int j=i; j<=i+2; j++) {
			try {
				//Socket inSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
				//		Integer.parseInt(nodes[j%5]) * 2);
				//inSocket.setSoTimeout(1000);
				Socket inSocket = new Socket();
				inSocket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(nodes[j % 5]) * 2), 5000);
				DataOutputStream out = new DataOutputStream(inSocket.getOutputStream());
				out.writeUTF("insert," + fileName + "," + content);
				out.flush();
				out.close();
				inSocket.close();
				//Log.v("insert","in the "+nodes[j%5]+ " " +values.toString());
			}catch(Exception e){
				Log.e("insertError", e.toString());
			}
		}*/
		//Log.v("insert", "in the "+ nodes[i]+":"+values.toString());
		Log.v("insert", values.toString());
		return uri;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
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
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		String[] table = new String[] {"key","value"};
		final MatrixCursor cursor = new MatrixCursor(table);
		if (selection.equals("*")) {
            /* add global data to cursor and return */
			String globalList[] = this.getContext().fileList();
			for (String _perKey : globalList) {
				try {
					FileInputStream q = getContext().openFileInput(_perKey);
					int l = q.available();
					byte[] reads = new byte[l];
					q.read(reads);
					String perVal = EncodingUtils.getString(reads, "UTF-8");
					cursor.addRow(new Object[]{_perKey, perVal.split("-")[0]});
				}catch(Exception e){
					Log.e("QueryGocalStep1", e.toString());
				}
			}
			int i = Arrays.asList(nodes).indexOf(portStr);
			String[] list = {nodes[(i+2)%5], nodes[(i+5-2)%5]};
			for(String per: list){
				try{
					Socket inSocket = new Socket();
					inSocket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(per) * 2), 1000);
					DataOutputStream out = new DataOutputStream(inSocket.getOutputStream());
					out.writeUTF("globalQuery");
					inSocket.setSoTimeout(1000);
					DataInputStream in = new DataInputStream(inSocket.getInputStream());
					String flow;
					while (true) {
						flow = in.readUTF();
						String content[] = flow.split(",");
						if (content.length == 2) {
							cursor.addRow(new Object[]{content[0], content[1]});
						} else if (content.length == 1) {
							return cursor;
						}
					}
				}catch(Exception e){
					Log.e("GlobalQException",e.toString());
				}
			}
		} else if(selection.equals("@")) {
            /* add local data to cursor and return */
			String globalList[] = this.getContext().fileList();
			for (String _perKey : globalList) {
				try {
					FileInputStream q = getContext().openFileInput(_perKey);
					int l = q.available();
					byte[] reads = new byte[l];
					q.read(reads);
					String perVal = EncodingUtils.getString(reads, "UTF-8");
					cursor.addRow(new Object[]{_perKey, perVal.split("-")[0]});
				}catch(Exception e){
					Log.e("QueryLocalException", e.toString());
				}
			}
		} else {
			String target = find(selection);
			int i = Arrays.asList(nodes).indexOf(target);
			/*while(true){
				try {
					Socket inSocket = new Socket();
					inSocket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(nodes[(i+2) % 5]) * 2), 1000);
					DataOutputStream out = new DataOutputStream(inSocket.getOutputStream());
					out.writeUTF("query," + selection);
					out.flush();
					inSocket.setSoTimeout(5000);
					DataInputStream in = new DataInputStream(inSocket.getInputStream());
					String content = in.readUTF();
					inSocket.close();
					cursor.addRow(new Object[]{selection, content});
					return cursor;
				} catch (Exception e) {
					Log.e("queryPer", e.toString());
				}
			}*/
			int version = 0;
			for (int j = i+2; j > i -1; j--) {
				try {
					Socket inSocket = new Socket();
					inSocket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(nodes[j % 5]) * 2), 1000);
					DataOutputStream out = new DataOutputStream(inSocket.getOutputStream());
					out.writeUTF("query," + selection);
					out.flush();
					inSocket.setSoTimeout(5000);
					DataInputStream in = new DataInputStream(inSocket.getInputStream());
					String content = in.readUTF();
					if(Integer.parseInt(content.split("-")[1])<version){
						continue;
					}else{
						version=Integer.parseInt(content.split("-")[1]);
						cursor.addRow(new Object[]{selection, content.split("-")[0]});
					}
					inSocket.close();
				} catch (Exception e) {
					Log.e("queryPer", e.toString());
				}
			}
		}
		Log.v("query", selection);
		return cursor;
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

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			Socket newSocket = null;
			DataInputStream in;
			String req = null;
			while(true) {
				try {
					newSocket = serverSocket.accept();
					in = new DataInputStream(newSocket.getInputStream());
					req = in.readUTF();
				}catch(Exception e){
					Log.e("ServerReadException", e.toString());
				}
				if(req != null){
					String request[] = req.split(",");
					String attribute = request[0];
					if(attribute.equals("insert")) {
						/*TODO: when insert, check if key exists, if yes, verison set to 0,
						 * if no, get value, and get the version number, version+1
						 * String keys[] = getContext().fileList();
							String thisVal;
							if(keys.length!=0 && Arrays.asList(keys).contains(key)){
								FileInputStream q = getContext().openFileInput(key);
								int l = q.available();
								byte[] reads = new byte[l];
								q.read(reads);
								String content = EncodingUtils.getString(reads, "UTF-8");
								thisVal = values+","+Integer.toString(Integer.parseInt(content.split("-")[1])+1);
							}else{
								thisVal = values+"-"+Integer.toString(0);
							}
						*/
						try {
							String key = request[1];
							String values = request[2];
							String keys[] = getContext().fileList();
							String thisVal;
							if(keys.length!=0 && Arrays.asList(keys).contains(key)){
								FileInputStream q = getContext().openFileInput(key);
								int l = q.available();
								byte[] reads = new byte[l];
								q.read(reads);
								String content = EncodingUtils.getString(reads, "UTF-8");
								thisVal = values+"-"+Integer.toString(Integer.parseInt(content.split("-")[1])+1);
							}else{
								thisVal = values+"-"+Integer.toString(0);
							}
							FileOutputStream save = getContext().openFileOutput(key, Context.MODE_PRIVATE);
							save.write(thisVal.getBytes());
							save.close();
							Log.v("insertDuplication", "key=" + key + " value=" + thisVal);
						}catch(Exception e){
							Log.e("ServerInsertError", e.toString());
						}
					}else if(attribute.equals("query")) {
						try {
							String key = request[1];
							FileInputStream q = getContext().openFileInput(key);
							int l = q.available();
							byte[] reads = new byte[l];
							q.read(reads);
							String content = EncodingUtils.getString(reads, "UTF-8");
							DataOutputStream out = new DataOutputStream(newSocket.getOutputStream());
							//out.writeUTF(content);
							out.writeUTF(content);
							//TODO: out.writeUTF(content.split("-")[0])
							out.flush();
							Log.v("queryme", key);
						}catch(Exception e){
							Log.e("ServerQueryError", e.toString());
						}
					} else if(attribute.equals("delete")){
						getContext().deleteFile(request[1]);
					} else if(attribute.equals("globalQuery")){
						try {
							String keys[] = getContext().fileList();
							DataOutputStream out = new DataOutputStream(newSocket.getOutputStream());
							for (String per : keys) {
								//if(isPartition(per,portStr)) {
								FileInputStream q = getContext().openFileInput(per);
								int l = q.available();
								byte[] reads = new byte[l];
								q.read(reads);
								String content = EncodingUtils.getString(reads, "UTF-8");
								//out.writeUTF(per + "," + content);
								out.writeUTF(per + "," + content.split("-")[0]);
								/*TODO:
								* out.writeUTF(per + "," + content.split(",")[0]);*/

								//}
							}
							int i = Arrays.asList(nodes).indexOf(portStr);
							out.writeUTF(nodes[(i + 1) % 5]);
						}catch(Exception e) {
							Log.e("ServerGlobalQueryError", e.toString());
						}
					} else if(attribute.equals("globalDelete")){
						String keys[] = getContext().fileList();
						for(String per: keys) {
							getContext().deleteFile(per);
						}
					} else if(attribute.equals("copyTo")){
						String your = request[1];
						int i = Arrays.asList(nodes).indexOf(your);
						String keys[] = getContext().fileList();
						//DataOutputStream out = new DataOutputStream(newSocket.getOutputStream());
						try {
							for (String per : keys) {
								if (isPartition(per, your)
										|| isPartition(per, nodes[(i + 5 - 1) % 5])
										|| isPartition(per, nodes[(i + 5 - 2) % 5])) {
									FileInputStream q = getContext().openFileInput(per);
									int l = q.available();
									byte[] reads = new byte[l];
									q.read(reads);
									String content = EncodingUtils.getString(reads, "UTF-8");
									DataOutputStream out = new DataOutputStream(newSocket.getOutputStream());
									out.writeUTF(per + "," + content);
								}
							}
							DataOutputStream out = new DataOutputStream(newSocket.getOutputStream());
							out.writeUTF("end");
						}catch(Exception e) {
							Log.e("ServerCopyError", e.toString());
						}
					}
				}
			}
			//return null;
		}

		protected void onProgressUpdate(String...strings) {
			return;
		}
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			/* while recovery, check all the keys, if keys recovered, and
			 * a new same key added, choose the version number which is larger
			 */
			int index = Arrays.asList(nodes).indexOf(portStr);
			String copyNode[] = {nodes[(index+4)%5], nodes[(index+1)%5], nodes[(index+2)%5], nodes[(index+3)%5]};
			for(String node: copyNode) {
				try {
					Socket socket = new Socket();
					socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(node) * 2), 10000);
					DataOutputStream out = new DataOutputStream(socket.getOutputStream());
					out.writeUTF("copyTo," + portStr);
					socket.setSoTimeout(10000);
					DataInputStream in = new DataInputStream(socket.getInputStream());
					String input;
					while (true) {
						input = in.readUTF();
						if (input.equals("end")) {
							socket.close();
							break;
						} else {
							String pairs[] = input.split(",");
							String key = pairs[0];
							String values = pairs[1];
							String keys[] = getContext().fileList();
							if(keys.length!=0 && Arrays.asList(keys).contains(key)){
								FileInputStream q = getContext().openFileInput(key);
								int l = q.available();
								byte[] reads = new byte[l];
								q.read(reads);
								String content = EncodingUtils.getString(reads, "UTF-8");
								int versionThis = Integer.parseInt(content.split("-")[1]);
								int versionRecv = Integer.parseInt(values.split("-")[1]);
								if(versionThis<versionRecv){
									FileOutputStream save = getContext().openFileOutput(key, Context.MODE_PRIVATE);
									save.write(values.getBytes());
									save.close();
								}
							}else{
								FileOutputStream save = getContext().openFileOutput(key, Context.MODE_PRIVATE);
								save.write(values.getBytes());
								save.close();
							}
						}
						Log.v("insertRecover", input);
					}
				}catch(Exception e){
					Log.e("recoverError", e.toString());
				}
			}
			return null;
		}
	}

	/*private class InsertTask extends AsyncTask<ContentValues, Void, Void> {

		@Override
		protected Void doInBackground(ContentValues... cv) {
			String fileName = cv[0].getAsString("key");
			String content = cv[0].getAsString("value");
			String coordinator = find(fileName);
			int i = Arrays.asList(nodes).indexOf(coordinator);
			for(int j=i; j<=i+2; j++) {
				try {
					//Socket inSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					//		Integer.parseInt(nodes[j%5]) * 2);
					//inSocket.setSoTimeout(1000);
					Socket inSocket = new Socket();
					inSocket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(nodes[j % 5]) * 2), 5000);
					DataOutputStream out = new DataOutputStream(inSocket.getOutputStream());
					out.writeUTF("insert," + fileName + "," + content);
					out.flush();
					out.close();
					//inSocket.close();
					//Log.v("insert","in the "+nodes[j%5]+ " " +values.toString());
				}catch(Exception e){
					Log.e("insertError", e.toString());
				}
			}
			return null;
		}
	}*/

	private String find(String key) {
		try {
			for (int i=0;i<nodes.length; i++){
				if (i==0 && (genHash(key).compareTo(genHash(nodes[0])) < 0 ||
						genHash(key).compareTo(genHash(nodes[4])) > 0)){
					return nodes[i];
				} else if(i>0 && genHash(key).compareTo(genHash(nodes[i-1])) > 0 &&
						genHash(key).compareTo(genHash(nodes[i])) < 0){
					return nodes[i];
				}
			}
		} catch (Exception e) {
			Log.e("findError", e.toString());

		}
		return null;
	}

	private boolean isPartition(String key, String port) {
		try {
			int index = Arrays.asList(nodes).indexOf(port);
			if (index == 0 && (genHash(key).compareTo(genHash(nodes[0])) < 0 ||
					genHash(key).compareTo(genHash(nodes[4])) > 0)) {
				return true;
			} else if(index > 0 && genHash(key).compareTo(genHash(nodes[index - 1])) > 0 &&
					genHash(key).compareTo(genHash(nodes[index])) < 0){
				return true;
			}
		} catch (Exception e){
			Log.e("isPartitionError",e.toString());
		}
		return false;
	}
}
