package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.w3c.dom.Node;

class Message implements Serializable {
	String type;
	String senderId = "0";
	int sendingto;
	HashMap<String, String> value = new HashMap<String, String>();
}

public class SimpleDynamoProvider extends ContentProvider {

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final int SERVER_PORT = 10000;
	static final String[] ports = {"5554", "5556", "5558", "5560", "5562"};
	static String[] NodeHash = new String[5];
	static String myPort;
	private Uri providerUri;
	static boolean failure = false;
	static int failed_avd = 5;

	static HashMap<String, String> querymap = new HashMap<String, String>();
	static HashMap<String, HashMap<String, String>> starmap = new HashMap<String, HashMap<String, String>>();
	//static HashMap<String, String> failuremap = new HashMap<String, String>();

	static final String Value_Insert = "InsertValue";
	static final String Query = "query";
	static final String Query_Reply = "queryreply";
	static final String StarQuery = "starquery";
	static final String StarReply = "starReply";
	static final String DeleteStar = "deleteS";
	static final String Delete = "delete";
	static final String Fail = "fail";
	static final String Recover = "recover";
	static final String RecoverValues = "recvalues";

	public void AnnounceFail(int a){
		Message fail_msg = new Message();
		fail_msg.type = Fail;
		fail_msg.sendingto = a;

		for (int i = 0; i < 5; i++) {

			try {
				if(i==a){
					continue;
				}
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(ports[i])*2);
				ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
				output.writeObject(fail_msg);
				output.flush();
				output.close();
				socket.close();

			} catch (UnknownHostException e) {
				Log.e(TAG, "Fail Broadcast UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "Fail Broadcast socket IOException");
			} catch (Exception e) {
				Log.e(TAG, "Fail Broadcast Exception");
				e.printStackTrace();
			}

		}
	}

	private int getInsertIndex(String keyhash) {
		int i;
		for (i = 0; i < 5; i++) {
			int j = i - 1;
			if (i == 0)
				j = 4;
			if ((keyhash.compareTo(NodeHash[i]) <= 0 && keyhash.compareTo(NodeHash[j]) > 0) || ((i == 0) && (keyhash.compareTo(NodeHash[j]) > 0 || keyhash.compareTo(NodeHash[i]) <= 0))) {
				break;
			}
		}
		return i;
	}

	private int getmyIndex(String port){
		int i;
		for(i=0; i<5; i++){
			if(ports[i].equals(port))
				break;
		}
		return i;
	}

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		Log.e(TAG, "Delete..................");

		if (selection.equals("@")) {
			Log.e(TAG, "Deleting all my files....");
			String[] files = getContext().fileList();
			for (int i = 0; i < files.length; i++) {

				File file = new File(files[i]);
				file.delete();
			}
		} else if (selection.equals("*")) {
			Log.e(TAG, "Deleting all my files....");
			String[] files = getContext().fileList();
			for (int i = 0; i < files.length; i++) {

				File file = new File(files[i]);
				file.delete();
			}
			Log.e(TAG, "Now sending delete * to others...");
			Message del = new Message();
			del.type = DeleteStar;
			del.senderId = myPort;
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, del);
		}

		else{
			Log.e(TAG, "Delete for: "+selection);
			try{
				Log.e(TAG, "Sending the Delete Request to the correct AVD's");
				int deleteindex = getInsertIndex(genHash(selection));
				Message del = new Message();
				del.type = Delete;
				del.senderId = selection;
				del.sendingto = deleteindex;
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, del);
			}catch (NoSuchAlgorithmException e) {
				Log.e(TAG, "No Such Algo....");
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
	public synchronized Uri insert(Uri uri, ContentValues values) {
		String keytoinsert = values.getAsString("key");
		String valuetoinsert = values.getAsString("value");
		Log.e(TAG, "In insert. For: "+keytoinsert+" Value: "+valuetoinsert );
		try {
			String keyhash = genHash(keytoinsert);
			int insertindex = getInsertIndex(keyhash);
			Log.e(TAG, "Should be inserted in: "+ports[insertindex]);
			Message insertmsg = new Message();
			insertmsg.type = Value_Insert;
			insertmsg.sendingto = insertindex;
			insertmsg.senderId = myPort;
			HashMap valueMap = new HashMap();
			valueMap.put("key",keytoinsert);
			valueMap.put("value",valuetoinsert);
			insertmsg.value = valueMap;
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insertmsg);

		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, "No Such Algo....");
		}
		return null;
	}

	@Override
	public boolean onCreate() {
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = portStr;
		providerUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

		try {
			for (int i = 0; i < 5; i++) {
				NodeHash[i] = genHash(ports[i]);
			}

			for(int i = 0; i<5; i++){
				for(int j = i+1; j<5; j++){
					if(NodeHash[i].compareTo(NodeHash[j])>0){
						String temp = NodeHash[i];
						String temp1 = ports[i];
						NodeHash[i] = NodeHash[j];
						ports[i] = ports[j];
						NodeHash[j] = temp;
						ports[j] = temp1;
					}
				}
			}

		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, "No Such Algo....");
		}

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			return false;
		}

		int a = getmyIndex(myPort);
		Message rec_msg = new Message();
		rec_msg.type = Recover;
		rec_msg.sendingto = a;
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, rec_msg);

		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {

		Log.e(TAG, "In Query...");
		MatrixCursor cursor = new MatrixCursor(new String[] {"key", "value"} );
		FileInputStream fileInputStream;
		byte[] input = new byte[512];
		StringBuffer value = new StringBuffer("") ;
		int b;

		if(selection.equals("@")){
			Log.v(TAG, "IN Query for @");
			String[] files = getContext().fileList();
			for(int i = 0; i<files.length; i++){
				String file = files[i];
				b = 0;
				value = new StringBuffer("");
				try{
					fileInputStream = getContext().openFileInput(file);
					while ((b=fileInputStream.read(input)) != -1){
						value.append(new String(input, 0, b));
					}
				}catch (Exception e) {
					Log.e(TAG, "Exception in Query for @...");

				}
				cursor.addRow(new String[] {file, value.toString()});
				Log.v("query for: ", file);
				Log.v("Value: ", value.toString());
			}
		}

		else if(selection.equals("*")){
			querymap.clear();
			starmap.clear();
			Log.v(TAG, "IN Query for *");
			String[] files = getContext().fileList();
			for (int i = 0; i < files.length; i++) {
				String file = files[i];
				b = 0;
				value = new StringBuffer("");
				try {
					fileInputStream = getContext().openFileInput(file);
					while ((b = fileInputStream.read(input)) != -1) {
						value.append(new String(input, 0, b));
					}
				} catch (Exception e) {
					Log.e(TAG, "Exception in Query for *...");
				}
				querymap.put(file, value.toString());

			}
			starmap.put(myPort, querymap);

			Message star = new Message();
			star.type = StarQuery;
			star.senderId = myPort;
			star.value.put("key", "@");
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, star);
			Log.e(TAG, "Waiting for a reply for * Query...");
			boolean k = false;
			while (!k) {
				k = (starmap.size()==5 || (failure && starmap.size()==4));
			}
			Log.e(TAG, "Done waiting.....");
			Iterator kit = starmap.keySet().iterator();
			while (kit.hasNext()) {
				String key = (String) kit.next();
				HashMap avdmap = starmap.get(key);
				Iterator avdit = avdmap.entrySet().iterator();
				while (avdit.hasNext()) {
					//HashMap map = (HashMap)avdit.next();
					Map.Entry avdq = (Map.Entry) avdit.next();
					String ky = (String) avdq.getKey();
					String v = (String) avdq.getValue();
					cursor.addRow(new String[]{ky, v});
				}
			}
		}

		else{
			querymap.clear();
			try{
				String keyhash = genHash(selection);
				int queryindex = getInsertIndex(keyhash);
				querymap.put(selection, null);
				Message querymsg = new Message();
				querymsg.type = Query;
				querymsg.senderId = myPort;
				querymsg.sendingto = queryindex;
				querymsg.value.put("key", selection);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, querymsg);
				Log.v("Waiting for query: ", selection);
				while (querymap.get(selection) == null){
				}
				Log.v("Done Waiting for ", selection);
				value.append(querymap.get(selection));

			}catch (Exception e) {
				Log.e(TAG, "Exception in Query....");
				e.printStackTrace();
			}

			cursor.addRow(new String[]{selection, value.toString()});
			Log.v("query for: ", selection);
			Log.v("Value: ", value.toString());
		}

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

			while (true) {
				try {
					Socket socket = serverSocket.accept();
					BufferedInputStream bin = new BufferedInputStream(socket.getInputStream());
					ObjectInputStream input = new ObjectInputStream(bin);
					Message msg = (Message) input.readObject();
					input.close();
					bin.close();
					socket.close();
					Log.e(TAG, "...................In Server");

					if (msg.type.equals(Value_Insert)) {
						Log.e(TAG, "In ServerTask. Got a new KeyValue. For key: " + msg.value.get("key"));
						String keytoinsert = msg.value.get("key");
						String valuetoinsert = msg.value.get("value");
						publishProgress(msg.type, keytoinsert, valuetoinsert);
					} else if (msg.type.equals(Query)||msg.type.equals(StarQuery)) {
						Log.e(TAG, "In ServerTask. Got a new Query. For key: " + msg.value.get("key"));
						String keytoinsert = msg.value.get("key");
						publishProgress(msg.type, msg.senderId, keytoinsert);
					} else if (msg.type.equals(Query_Reply)) {
						Log.e(TAG, "In ServerTask. Got a reply for my Query for: " + msg.value.get("key"));
						String key = msg.value.get("key");
						String val = msg.value.get("value");
						querymap.put(key, val);
					}
					else if(msg.type.equals(StarReply)){
						Log.e(TAG, "In ServerTask. Got a reply from someone for *.... Updating my StarMap.");
						starmap.put(msg.senderId, msg.value);
						Log.e(TAG, "Length of starmap is now: " + starmap.size());
					}
					else if(msg.type.equals(DeleteStar)){
						publishProgress(msg.type);
					}
					else if(msg.type.equals(Delete)){
						publishProgress(msg.type, msg.senderId);
					}
					else if(msg.type.equals(Fail)){
						Log.e(TAG, "In ServerTask. Got Failure msg for: "+msg.sendingto);
						failure = true;
						failed_avd = msg.sendingto;
					}
					else if(msg.type.equals(Recover)){
						Log.e(TAG, "In ServerTask. Got a Recovery msg for: "+msg.sendingto);
						Log.e(TAG, "Failure: "+failure+" Failed AVD: "+failed_avd);

						if(failure && failed_avd==msg.sendingto){
							Log.e(TAG, "Updated my Failure variable for: "+msg.sendingto);
							failure = false;
							failed_avd = 5;
							int send[] = new int[2];
							send[0] = msg.sendingto-1;
							send[1] = msg.sendingto+1;
							if(msg.sendingto == 0)
								send[0] = 4;
							if(msg.sendingto == 4)
								send[1] = 0;
							if(getmyIndex(myPort) == send[0] || getmyIndex(myPort) == send[1])
								publishProgress(msg.type, ports[msg.sendingto]);
						}
					}
					else if(msg.type.equals(RecoverValues)){
						Log.e(TAG, "In ServerTask. Updating my entries after recovery. Map from: "+msg.senderId+" Size: "+msg.value.size());
						Iterator rec_it = msg.value.entrySet().iterator();
						while (rec_it.hasNext()) {
							Map.Entry avdq = (Map.Entry) rec_it.next();
							String k = (String) avdq.getKey();
							String v = (String) avdq.getValue();
							Log.e(TAG, "Key: "+k+" Value: "+v);
							synchronized(this) {
								FileOutputStream outputStream;
								try {
									outputStream = getContext().openFileOutput(k, Context.MODE_PRIVATE);
									outputStream.write(v.getBytes());
									outputStream.flush();
									outputStream.close();
								} catch (Exception e) {
									Log.e(TAG, "File write failed");
								}
							}
						}
					}

				} catch (IOException e) {
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
		}

		protected void onProgressUpdate(String... strings) {
			String status = strings[0];
			Log.e(TAG, "In ServerTask. Status of incoming msg= " + status);

			if (status.equals(Value_Insert)) {
				synchronized(this) {
				FileOutputStream outputStream;
				try {
					outputStream = getContext().openFileOutput(strings[1], Context.MODE_PRIVATE);
					outputStream.write(strings[2].getBytes());
					outputStream.flush();
					outputStream.close();
				} catch (Exception e) {
					Log.e(TAG, "File write failed");
				}
				}
			}

			else if (status.equals(Query)) {
				String sendto = strings[1];
				String Qkey = strings[2];

				int b = 0;
				FileInputStream fileInputStream;
				byte[] input = new byte[512];
				StringBuffer value = new StringBuffer("") ;
				try {
					fileInputStream = getContext().openFileInput(Qkey);

					while ((b = fileInputStream.read(input)) != -1 || value.toString() == null || value.toString() == "") {
						value.append(new String(input, 0, b));
					}
					Log.e(TAG, "I Have the result for Query. For: " + Qkey + " Value: " + value.toString());
					String result = value.toString();

					if (result != null && result != "") {
						Message replymsg = new Message();
						replymsg.type = Query_Reply;
						replymsg.value.put("key", Qkey);
						replymsg.value.put("value", result);
						replymsg.senderId = myPort;
						replymsg.sendingto = getmyIndex(sendto);
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, replymsg);
					}
				} catch (Exception e) {
					Log.e(TAG, "Serve...Exception in Query....");
					e.printStackTrace();
				}

			}
			else if(status.equals(StarQuery)){
				Cursor resultcursor = query(providerUri, null, strings[2], null, null);
				HashMap<String, String> star = new HashMap();
				int keyIndex = resultcursor.getColumnIndex("key");
				int valueIndex = resultcursor.getColumnIndex("value");
				if(resultcursor.getCount()>0){
					resultcursor.moveToFirst();
					String returnKey = resultcursor.getString(keyIndex);
					String returnValue = resultcursor.getString(valueIndex);
					star.put(returnKey, returnValue);

					while(resultcursor.moveToNext()){
						returnKey = resultcursor.getString(resultcursor.getColumnIndex("key"));
						returnValue = resultcursor.getString(resultcursor.getColumnIndex("value"));
						star.put(returnKey, returnValue);
					}
				}
				resultcursor.close();
				Message starreply = new Message();
				starreply.type = StarReply;
				starreply.value = star;
				starreply.senderId = myPort;
				starreply.sendingto = getmyIndex(strings[1]);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, starreply);
			}
			else if(status.equals(DeleteStar)){
				delete(providerUri, "@", null);
			}
			else if(status.equals(Delete)){
				File dir = getContext().getFilesDir();
				File file = new File(dir, strings[1]);
				if(file.exists()){
					Log.e(TAG, "Deleting. I have the file for: "+strings[1]);
					file.delete();
				}
				else
					Log.e(TAG, "Got a Delete Request. But I Don't have the file for: "+strings[1]);

			}
			else if(status.equals(Recover)){

				Message recovervalues = new Message();
				recovervalues.type = RecoverValues;
				recovervalues.sendingto = getmyIndex(strings[1]);
				recovervalues.senderId = myPort;

				int me = getmyIndex(myPort);

				int b;
				StringBuffer value;
				FileInputStream fileInputStream;
				byte[] input = new byte[512];
				String[] files = getContext().fileList();
				for(int i = 0; i<files.length; i++) {

					try {
						String file = files[i];
					int q = getInsertIndex(genHash(file));
					boolean w = false;
					for(int j = 0; j<3; j++){
						if(recovervalues.sendingto == (q+j)%5){
							w = true;
							break;
						}
					}
					if(w){
						b = 0;
						value = new StringBuffer("");

						fileInputStream = getContext().openFileInput(file);
						while ((b = fileInputStream.read(input)) != -1) {
							value.append(new String(input, 0, b));
						}
						recovervalues.value.put(file, value.toString());
					}

					} catch (Exception e) {
						Log.e(TAG, "Exception in Query for @...");

					}
				}
				Log.e(TAG, "Actual size of map: "+recovervalues.value.size());
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, recovervalues);

			}
		}
	}

	private class ClientTask extends AsyncTask<Message, Void, Void> {
		@Override
		protected Void doInBackground(Message... msgs) {

			Message msg = msgs[0];

			if(msg.type.equals(Value_Insert)||msg.type.equals(Query)||msg.type.equals(Delete)){
				for(int i=0; i<3; i++){
					int sendport = (msg.sendingto+i)%5;
					try{
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(ports[sendport])*2));
						ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
						Log.e(TAG, "in Client Task. Sending read/write msg to: "+ports[sendport]);
						output.writeObject(msg);
						output.flush();
						output.close();
						socket.close();
					} catch (UnknownHostException e) {
						Log.e(TAG, "ClientTask UnknownHostException");
					} catch (IOException e) {
						Log.e(TAG, "ClientTask socket IOException");
						if(failed_avd!=sendport)
							AnnounceFail(sendport);
					} catch (Exception e) {
						Log.e(TAG, "Parent Exception");
						e.printStackTrace();
					}
				}
			}

			else if(msg.type.equals(Query_Reply)||msg.type.equals(StarReply)|| msg.type.equals(RecoverValues)){
				try{
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(ports[msg.sendingto])*2));
					ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
					Log.e(TAG, "in Client Task. Sending query msg to: "+ports[msg.sendingto]);
					output.writeObject(msg);
					output.flush();
					output.close();
					socket.close();
				} catch (UnknownHostException e) {
					Log.e(TAG, "ClientTask UnknownHostException");
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException");
					if(failed_avd!=msg.sendingto)
						AnnounceFail(msg.sendingto);
				} catch (Exception e) {
					Log.e(TAG, "Parent Exception");
					e.printStackTrace();
				}
			}

			else if(msg.type.equals(StarQuery)||msg.equals(DeleteStar)|| msg.type.equals(Recover)){
				for(int i=0; i<5; i++){
					int me = getmyIndex(myPort);
					if(i==me)
						continue;
					try{
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(ports[i])*2));
						ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
						Log.e(TAG, "in Client Task. Sending msg to everyone: "+ports[i]);
						output.writeObject(msg);
						output.flush();
						output.close();
						socket.close();
					} catch (UnknownHostException e) {
						Log.e(TAG, "ClientTask UnknownHostException");
					} catch (IOException e) {
						Log.e(TAG, "ClientTask socket IOException");
						if(failed_avd!=i)
							AnnounceFail(i);
					} catch (Exception e) {
						Log.e(TAG, "Parent Exception");
						e.printStackTrace();
					}
				}
			}
			return null;
		}
	}
}