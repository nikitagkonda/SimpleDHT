package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Formatter;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final int SERVER_PORT = 10000;
    static String myPort;

    private static final String KEY = "key";
    private static final String VALUE = "value";
    private static final String TABLE_NAME = "messages";
    /*
    *Reference: https://developer.android.com/guide/topics/providers/content-provider-creating.html
    */

    private SimpleDhtDatabaseHelper dbHelper;

    // Holds the database object
    private SQLiteDatabase db;

    private final Uri mUri= buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

     static ArrayList<Nodes> nodeRing = new ArrayList<Nodes>();

     static Comparator<Nodes> comparator = new Comparator<Nodes>() {
        public int compare(Nodes n1, Nodes n2) {
            return n1.getId().compareTo(n2.getId());
        }
    };

    private class Nodes {

        String id;
        String port_no;

        public Nodes(String id, String port_no) {
            this.id = id;
            this.port_no = port_no;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getPort_no() {
            return port_no;
        }

        public void setPort_no(String port_no) {
            this.port_no = port_no;
        }
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        db =dbHelper.getWritableDatabase();

        if(nodeRing.size()==0 || nodeRing.size()==1) {
            if(!selection.equals("@") && !selection.equals("*"))
                db.delete(TABLE_NAME, KEY + "='" + selection + "'", null);
            else
                db.delete(TABLE_NAME, null,null);
        }
        else {
            if(selection.equals("@")){
                db.delete(TABLE_NAME, null,null);
            } else if(selection.equals("*")){
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "deleteall");
            } else {
                String keyHash = null;
                try {
                    keyHash = genHash(selection);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                boolean deleteFlag=false;
                for (int i=0;i<nodeRing.size();i++) {
                    if(i==0){
                        if(keyHash.compareTo(nodeRing.get(i).getId()) <= 0 || keyHash.compareTo(nodeRing.get(nodeRing.size()-1).getId())>0) {
                            deleteFlag =true;
                        }
                    }
                    else if (keyHash.compareTo(nodeRing.get(i).getId()) <= 0 && keyHash.compareTo(nodeRing.get(i-1).getId())>0){
                        deleteFlag=true;
                    }

                    if(deleteFlag) {
                        if(myPort.equals(nodeRing.get(i).getPort_no())){
                            db.delete(TABLE_NAME, KEY + "='" + selection + "'", null);
                        }
                        else {
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "delete", nodeRing.get(i).getPort_no(), selection);
                        }
                        break;
                    }
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
        // TODO Auto-generated method stub

         /*
        Reference 1: https://developer.android.com/reference/android/database/sqlite/SQLiteQueryBuilder.html
        Reference 2: https://developer.android.com/reference/android/database/sqlite/SQLiteDatabase.html
         */
        db = dbHelper.getWritableDatabase();

        String keyHash = null;
        try {
            keyHash = genHash(values.getAsString(KEY));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if((nodeRing.size()==0 || nodeRing.size()==1)){
            SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
            queryBuilder.setTables(TABLE_NAME);
            Cursor cursor = queryBuilder.query(db, null, null, null, null, null, null);
            boolean updateFlag = false;
            while (cursor.moveToNext()) {
                String s = cursor.getString(0);
                if (s.equals(values.getAsString(KEY))) {
                    updateFlag = true;
                    break;
                }
            }

            cursor.close();
            if (updateFlag) {
                int updaterow = db.update(TABLE_NAME, values, KEY + "='" + values.getAsString(KEY) + "'", null);
//                Log.v("update", Integer.toString(updaterow));
            } else {
                long id = db.insert(TABLE_NAME, null, values);
//                Log.v("insert", Long.toString(id));
            }
        } else {
            boolean insertFlag=false;
            for (int i=0;i<nodeRing.size();i++) {
                if(i==0){
                    if(keyHash.compareTo(nodeRing.get(i).getId()) <= 0 || keyHash.compareTo(nodeRing.get(nodeRing.size()-1).getId())>0) {
                        insertFlag =true;

                    }
                }
                else if (keyHash.compareTo(nodeRing.get(i).getId()) <= 0 && keyHash.compareTo(nodeRing.get(i-1).getId())>0){
                    insertFlag=true;
                }

                if(insertFlag) {
                    if (myPort.equals(nodeRing.get(i).getPort_no())) {

                        SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
                        queryBuilder.setTables(TABLE_NAME);
                        Cursor cursor = queryBuilder.query(db, null, null, null, null, null, null);
                        boolean updateFlag = false;
                        while (cursor.moveToNext()) {
                            String s = cursor.getString(0);
                            if (s.equals(values.getAsString(KEY))) {
                                updateFlag = true;
                                break;
                            }
                        }

                        cursor.close();
                        if (updateFlag) {
                            int updaterow = db.update(TABLE_NAME, values, KEY + "='" + values.getAsString(KEY) + "'", null);
//                            Log.v("update", Integer.toString(updaterow));
                        } else {
                            long id = db.insert(TABLE_NAME, null, values);
//                            Log.v("insert", Long.toString(id));
                        }
                    } else {
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", nodeRing.get(i).getPort_no(), values.getAsString(KEY), values.getAsString(VALUE));
                    }

                    break;
                }

            }
        }
        return uri;


    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        //Reference: https://developer.android.com/guide/topics/providers/content-provider-creating.html
        dbHelper = new SimpleDhtDatabaseHelper(
                getContext()       // the application context
        );

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             *
             * AsyncTask is a simplified thread construct that Android provides. Please make sure
             * you know how it works by reading
             * http://developer.android.com/reference/android/os/AsyncTask.html
             */
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            /*
             * Log is a good way to debug your code. LogCat prints out all the messages that
             * Log class writes.
             *
             * Please read http://developer.android.com/tools/debugging/debugging-projects.html
             * and http://developer.android.com/tools/debugging/debugging-log.html
             * for more information on debugging.
             */
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "node join", myPort);

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub

        /*
        Reference: https://developer.android.com/reference/android/database/sqlite/SQLiteQueryBuilder.html
         */

        db=dbHelper.getReadableDatabase();
        SQLiteQueryBuilder queryBuilder= new SQLiteQueryBuilder();
        queryBuilder.setTables(TABLE_NAME);

        if((nodeRing.size()==0 || nodeRing.size()==1)){
            if(!selection.equals("*") && !selection.equals("@"))
                queryBuilder.appendWhere(KEY+"='"+selection+"'");
            return queryBuilder.query(db,projection,null,selectionArgs,null,null,null);
        }

        else if(selection.equals("@"))
        {
            return queryBuilder.query(db,projection,null,selectionArgs,null,null,null);
        }

        else if(!selection.equals("*")){
            queryBuilder.appendWhere(KEY+"='"+selection+"'");
            String keyHash=null;
            try {
                keyHash=genHash(selection);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            boolean queryFlag=false;
            for (int i=0;i<nodeRing.size();i++) {
                if(i==0){
                    if(keyHash.compareTo(nodeRing.get(i).getId()) <= 0 || keyHash.compareTo(nodeRing.get(nodeRing.size()-1).getId())>0) {
                        queryFlag =true;

                    }
                }
                else if (keyHash.compareTo(nodeRing.get(i).getId()) <= 0 && keyHash.compareTo(nodeRing.get(i-1).getId())>0){
                    queryFlag=true;
                }

                if(queryFlag) {
                    if (myPort.equals(nodeRing.get(i).getPort_no())) {

                        //                        Log.v("return",String.valueOf(cursor.getCount()));
                        return queryBuilder.query(db,projection,null,selectionArgs,null,null,null);
                    }
                    else {
                        String[] result = null;
                        try {
                            result = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query", nodeRing.get(i).getPort_no(), selection).get();

                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (ExecutionException e) {
                            e.printStackTrace();
                        }
                        MatrixCursor cursor = new MatrixCursor(new String[] { KEY, VALUE });
                        if (result != null) {
                            cursor.addRow(new Object[] { result[0], result[1] });
                        }
                        return cursor;
                    }
                }

            }
        } else if(selection.equals("*")){

            String[] result = null;

            try {
                result = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "queryall").get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            MatrixCursor cursor = new MatrixCursor(new String[] { KEY, VALUE });
            if (result != null) {
                for(String r : result){
                    String[] temp = r.split(",");
                    cursor.addRow(new Object[] { temp[0], temp[1]});
                }
            }

            return cursor;

        }

        return null;

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

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */

            while(true) {
                try {
                    Socket server = serverSocket.accept();
                    ObjectInputStream ois = new ObjectInputStream(server.getInputStream());
                    Object o = ois.readObject();
                    String[] received = (String[]) o;

                    if (received[0].equals("node join")) {
                        String node_id = genHash(String.valueOf(Integer.parseInt(received[1])/2));
                        Nodes node = new Nodes(node_id, received[1]);
                        nodeRing.add(node);
                        Collections.sort(nodeRing, comparator);
                        publishProgress("pass");
                    }
                    else if(received[0].equals("node ring")){
                        String[] node_ring = received[1].split(",");
                        if(!myPort.equals(REMOTE_PORT0)) {
                            nodeRing.clear();
                            for (String aNode_ring : node_ring) {
                                String node_id = genHash(String.valueOf(Integer.parseInt(aNode_ring) / 2));
                                Nodes node = new Nodes(node_id, aNode_ring);
                                nodeRing.add(node);
                            }
                        }
                    }
                    else if(received[0].equals("insert")){
                        publishProgress(received);
                    }

                    else if(received[0].equals("query")){
                        Cursor resultCursor = getContext().getContentResolver().query(mUri, null,
                                received[2], null, null);
                        String[] result = new String[2];
                        while(resultCursor.moveToNext()){
                            result[0]= resultCursor.getString(0);
                            result[1] = resultCursor.getString(1);
                        }

                        resultCursor.close();

                        ObjectOutputStream oos = new ObjectOutputStream(server.getOutputStream());
                        oos.writeObject(result);


                    }
                    else if(received[0].equals("queryall")){
                        Cursor resultCursor = getContext().getContentResolver().query(mUri, null,
                                "@", null, null);
                        ArrayList<String> listResult = new ArrayList<String>();

                        while(resultCursor.moveToNext()){
                            String result= resultCursor.getString(0) + "," + resultCursor.getString(1);
                            listResult.add(result);
                        }

                        resultCursor.close();

                        String [] result = listResult.toArray(new String[listResult.size()]);

                        ObjectOutputStream oos = new ObjectOutputStream(server.getOutputStream());
                        oos.writeObject(result);
                    }
                    else if(received[0].equals("deleteall")){
                        int i = getContext().getContentResolver().delete(mUri,"@",null);
                    }
                    else if(received[0].equals("delete")){
                        int i = getContext().getContentResolver().delete(mUri,received[2],null);
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }
        }

        protected void onProgressUpdate(String...strings) {

            if(strings[0].equals("pass")){
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "node ring",REMOTE_PORT0);
            }
            else if(strings[0].equals("insert"))
            {
                ContentValues values= new ContentValues();
                values.put("key",strings[2]);
                values.put("value",strings[3]);
                Uri newUri = getContext().getContentResolver().insert(mUri, values);
            }

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
    private class ClientTask extends AsyncTask<String, Void, String[]> {

        @Override
        protected String[] doInBackground(String... msgs) {
            try {
                if(msgs[0].equals("node join")) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(REMOTE_PORT0));

                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    oos.writeObject(msgs);
                    try {
                        Thread.sleep(300);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    socket.close();
                }
                else if(msgs[0].equals("node ring")){

                    String nodeRingString = null;
                    for(Nodes nodes: nodeRing ){
                        if(nodeRingString==null)
                            nodeRingString=nodes.getPort_no();
                        else
                            nodeRingString=nodeRingString+","+nodes.getPort_no();
                    }
                    String[] msgToSend = new String[3];
                    msgToSend[0]=msgs[0];
                    msgToSend[1]=nodeRingString;
                    try{
                    for(Nodes n: nodeRing){

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(n.getPort_no()));
                        msgToSend[2]=n.getPort_no();
                        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                        oos.writeObject(msgToSend);

                            Thread.sleep(300);

                        socket.close();
                    }} catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ConcurrentModificationException e) {
                        e.printStackTrace();
                    }
                }
                else if(msgs[0].equals("insert")){

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[1]));

                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    oos.writeObject(msgs);
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    socket.close();



                }

                else if(msgs[0].equals("query")){
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[1]));

                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    oos.writeObject(msgs);
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                    String[] result = (String[]) ois.readObject();
                    socket.close();
                    return result;

                }
                else if(msgs[0].equals("queryall")){

                    ArrayList<String[]> listResult = new ArrayList<String[]>();
                    for(int i=0;i<nodeRing.size();i++) {

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(nodeRing.get(i).getPort_no()));

                        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                        oos.writeObject(msgs);
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                        listResult.add((String[]) ois.readObject());
                        socket.close();

                    }

                    ArrayList<String> temp = new ArrayList<String>();

                    for(String[] x: listResult){
                        Collections.addAll(temp, x);
                    }
                    return temp.toArray(new String[temp.size()]);
                }
                else if(msgs[0].equals("deleteall")){
                    for(int i=0;i<nodeRing.size();i++) {

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(nodeRing.get(i).getPort_no()));

                        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                        oos.writeObject(msgs);
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        socket.close();

                    }
                }
                else if(msgs[0].equals("delete")){
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[1]));

                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    oos.writeObject(msgs);
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    socket.close();
                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            return null;
        }
    }
}
