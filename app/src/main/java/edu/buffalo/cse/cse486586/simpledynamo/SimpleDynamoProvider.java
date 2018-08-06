package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Formatter;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

public class SimpleDynamoProvider extends ContentProvider {

    private Database database;
    private String myPort;
    private String emuNumber;
    private static final int SERVER_PORT = 10000;
    private static List<String> portList = Arrays.asList("11124", "11112", "11108", "11116", "11120");
    private static List<String> hashList = Arrays.asList(
            "177ccecaec32c54b82d5aaafc18a2dadb753e3b1",
            "208f7f72b198dadd244e61801abe1ec3a4857bc9",
            "33d6357cfaaf0f72991b0ecd8c56da066613c089",
            "abf0fd8db03e5ecb199a9b82929e9db79b909643",
            "c25ddd596aa7c81fa12378fa725f706d54325d12");

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        if (selection.equals("@")) {
            database.deleteAll();
        } else {
            Message message = new Message();
            if (selection.equals("*")) {
                message.setMessageStatus(MessageStatus.DELETE_ALL);
                try {
                    new ClientDeleteAllTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message).get();
                    database.deleteAll();
                } catch (InterruptedException e) {
                    Log.e("Delete", e.getStackTrace().toString());
                } catch (ExecutionException e) {
                    Log.e("Delete", e.getStackTrace().toString());
                }
            } else {
                String owner = getOwnerPort(selection);
                String[] ports = getNextTwoPorts(owner);
                message.setMessageStatus(MessageStatus.DELETE);
                message.setKey(selection);
                if (owner.equals(myPort)) {
                    database.delete(selection);
                } else {
                    try {
                        new ClientDeleteTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, owner).get();
                    } catch (InterruptedException e) {
                        Log.e("Delete", e.getStackTrace().toString());
                    } catch (ExecutionException e) {
                        Log.e("Delete", e.getStackTrace().toString());
                    }
                }
                for (String port : ports) {
                    try {
                        new ClientDeleteTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, port).get();
                    } catch (InterruptedException e) {
                        Log.e("Delete", e.getStackTrace().toString());
                    } catch (ExecutionException e) {
                        Log.e("Delete", e.getStackTrace().toString());
                    }
                }
            }
        }
        return 1;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String ownerPort = getOwnerPort((String) values.get("key"));
        Message message = new Message();
        message.setMessageStatus(MessageStatus.INSERT);
        message.setKey((String) values.get("key"));
        message.setValue((String) values.get("value"));
        message.setVersion("1");
        if (ownerPort.equals(myPort)) {
            database.insert(values);
        } else {
            sendToOwner(ownerPort, message);
        }
        replicate(message, getNextTwoPorts(ownerPort));
        return uri;
    }

    private void sendToOwner(String ownerPort, Message message) {
        try {
            new ClientInsertTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, new String[]{ownerPort}).get();
        } catch (InterruptedException e) {
            Log.e("Send to owner", e.getStackTrace().toString());
        } catch (ExecutionException e) {
            Log.e("Send to owner", e.getStackTrace().toString());
        }
    }

    private void replicate(Message message, String ports[]) {
        try {
            new ClientInsertTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, ports).get();
        } catch (InterruptedException e) {
            Log.e("Replicate", e.getStackTrace().toString());
        } catch (ExecutionException e) {
            Log.e("Replicate", e.getStackTrace().toString());
        }
    }

    private String[] getNextTwoPorts(String port) {
        int myIndex = portList.indexOf(port);
        String[] ports = new String[2];
        ports[0] = portList.get((myIndex + 1) % hashList.size());
        ports[1] = portList.get((myIndex + 2) % hashList.size());
        return ports;
    }

    private String[] getPreviousTwoPorts(String port) {
        int myIndex = portList.indexOf(port);
        String[] ports = new String[2];
        int prevIndex1 = (myIndex - 1) % hashList.size();
        int prevIndex2 = (myIndex - 2) % hashList.size();
        if (prevIndex1 < 0)
            ports[0] = portList.get(prevIndex1 + hashList.size());
        else
            ports[0] = portList.get(prevIndex1);
        if (prevIndex2 < 0)
            ports[1] = portList.get(prevIndex2 + hashList.size());
        else
            ports[1] = portList.get(prevIndex2);
        return ports;
    }

    private String getOwnerPort(String key) {
        try {
            String keyHash = genHash(key);
            if (keyHash.compareTo(hashList.get(0)) <= 0 || keyHash.compareTo(hashList.get(hashList.size() - 1)) > 0) {
                return portList.get(0);
            } else {
                for (int i = 1; i < hashList.size(); i++) {
                    if (keyHash.compareTo(hashList.get(i)) <= 0) {
                        return portList.get(i);
                    }
                }
            }
        } catch (NoSuchAlgorithmException e) {
            Log.e("Owner Port", e.getStackTrace().toString());
        }
        return null;
    }

    @Override
    public boolean onCreate() {
        database = new Database(getContext());
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        emuNumber = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(emuNumber) * 2));
        try {
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, new ServerSocket(SERVER_PORT));
            List<Message> messages = new ArrayList<Message>();
            Message message = new Message();
            message.setMessageStatus(MessageStatus.QUERY_ALL);
            try {
                List<Message> allMessages = new ClientQueryAllTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, Arrays.asList(getNextTwoPorts(myPort))).get();
                for (Message m : allMessages) {
                    if (getOwnerPort(m.getKey()).equals(myPort)) {
                        messages.add(m);
                    }
                }
            } catch (InterruptedException e) {
                Log.e("On Create ", e.getStackTrace().toString());
            } catch (ExecutionException e) {
                Log.e("On Create ", e.getStackTrace().toString());
            }

            try {
                List<Message> allMessages = new ClientQueryAllTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, Arrays.asList(getPreviousTwoPorts(myPort))).get();
                for (Message m : allMessages) {
                    if (getOwnerPort(m.getKey()).equals(getPreviousTwoPorts(myPort)[0]) || getOwnerPort(m.getKey()).equals(getPreviousTwoPorts(myPort)[1])) {
                        messages.add(m);
                    }
                }
            } catch (InterruptedException e) {
                Log.e("On Create ", e.getStackTrace().toString());
            } catch (ExecutionException e) {
                Log.e("On Create ", e.getStackTrace().toString());
            }

            for (Message m : messages) {
                ContentValues contentValues = new ContentValues();
                contentValues.put("key", m.getKey());
                contentValues.put("value", m.getValue());
                contentValues.put("version",m.getVersion());
                database.insert(contentValues);
            }
        } catch (IOException e) {
            Log.e("On Create ", e.getStackTrace().toString());
        }
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        if (selection.equals("@")) {
            return database.getAllMessages();
        } else {
            Message message = new Message();
            if (selection.equals("*")) {
                message.setMessageStatus(MessageStatus.QUERY_ALL);
                try {
                    List<Message> messages = new ClientQueryAllTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, portList).get();
                    messages.addAll(getListFromCursor(database.getAllMessages()));
                    return buildCursor(messages);
                } catch (InterruptedException e) {
                    Log.e("Query", e.getStackTrace().toString());
                } catch (ExecutionException e) {
                    Log.e("Query", e.getStackTrace().toString());
                }
            } else {
                String owner = getOwnerPort(selection);
                String[] ports = getNextTwoPorts(owner);
                List<Message> messages = new ArrayList<Message>();
                message.setMessageStatus(MessageStatus.QUERY);
                message.setKey(selection);
                if (owner.equals(myPort)) {
                    messages.add(buildMessage(database.query(selection)));
                } else {
                    try {
                        messages.add(new ClientQueryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, owner).get());
                    } catch (InterruptedException e) {
                        Log.e("Query", e.getStackTrace().toString());
                    } catch (ExecutionException e) {
                        Log.e("Query", e.getStackTrace().toString());
                    }
                }
                for (String port : ports) {
                    try {
                        messages.add(new ClientQueryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, port).get());
                    } catch (InterruptedException e) {
                        Log.e("Query", e.getStackTrace().toString());
                    } catch (ExecutionException e) {
                        Log.e("Query", e.getStackTrace().toString());
                    }
                }

                for (Message m : messages) {
                    if (m != null)
                        if (m.getMessageStatus() == null)
                            return buildCursor(m);
                }
            }
        }
        return null;
    }

    public List<Message> getListFromCursor(Cursor cursor) {
        List<Message> list = new ArrayList();
        cursor.moveToFirst();
        while (!cursor.isAfterLast()) {
            Message message = new Message();
            message.setKey(cursor.getString(0));
            message.setValue(cursor.getString(1));
            message.setVersion(cursor.getString(2));
            list.add(message);
            cursor.moveToNext();
        }
        return list;
    }

    public Message buildMessage(Cursor cursor) {
        Message message = new Message();
        cursor.moveToFirst();
        message.setKey(cursor.getString(0));
        message.setValue(cursor.getString(1));
        message.setVersion(cursor.getString(2));
        return message;
    }

    Cursor buildCursor(Message message) {
        MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
        cursor.addRow(new String[]{message.getKey(), message.getValue()});
        return cursor;
    }

    Cursor buildCursor(List<Message> messages) {
        MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
        for (Message message : messages)
            cursor.addRow(new String[]{message.getKey(), message.getValue()});
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

    class ClientInsertTask extends AsyncTask<Object, Void, Void> {
        @Override
        protected Void doInBackground(Object... objects) {
            Message message = (Message) objects[0];
            String[] ports = (String[]) objects[1];
            for (String port : ports) {
                Socket socket = null;
                try {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.valueOf(port));
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                    objectOutputStream.writeObject(message);
                    objectOutputStream.flush();
                } catch (IOException e) {
                    Log.e("Client Insert Task", e.getStackTrace().toString());
                }
            }
            return null;
        }
    }

    class ClientQueryTask extends AsyncTask<Object, Void, Message> {
        @Override
        protected Message doInBackground(Object... objects) {
            Message message = (Message) objects[0];
            String owner = (String) objects[1];
            Socket socket = null;
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.valueOf(owner));
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                objectOutputStream.writeObject(message);
                objectOutputStream.flush();
                ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
                try {
                    message = (Message) objectInputStream.readObject();
                    return message;
                } catch (ClassNotFoundException e) {
                    Log.e("Client Query Task", e.getStackTrace().toString());
                }
                objectInputStream.close();
                objectOutputStream.close();
                socket.close();
            } catch (IOException e) {
                Log.e("Client Query Task", e.getStackTrace().toString());
            }

            return null;
        }
    }

    class ClientDeleteTask extends AsyncTask<Object, Void, Void> {
        @Override
        protected Void doInBackground(Object... objects) {
            Message message = (Message) objects[0];
            String owner = (String) objects[1];
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.valueOf(owner));
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                objectOutputStream.writeObject(message);
                objectOutputStream.flush();
            } catch (IOException e) {
                Log.e("Client Delete Task", e.getStackTrace().toString());
            }
            return null;
        }
    }

    class ClientDeleteAllTask extends AsyncTask<Message, Void, Void> {
        @Override
        protected Void doInBackground(Message... messages) {
            Message message = messages[0];
            for (String port : portList) {
                if (port.equals(myPort)) continue;
                Socket socket = null;
                try {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.valueOf(port));
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                    objectOutputStream.writeObject(message);
                    objectOutputStream.flush();
                } catch (IOException e) {
                    Log.e("Client Delete All Task", e.getStackTrace().toString());
                }

            }
            return null;
        }
    }

    class ClientQueryAllTask extends AsyncTask<Object, Void, List<Message>> {
        @Override
        protected List<Message> doInBackground(Object... objects) {
            List<Message> messageList = new ArrayList<Message>();
            Message message = (Message) objects[0];
            List<String> portList = (List<String>) objects[1];
            for (String port : portList) {
                if (port.equals(myPort)) continue;
                Socket socket = null;
                try {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.valueOf(port));
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                    objectOutputStream.writeObject(message);
                    objectOutputStream.flush();
                    ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
                    messageList.addAll((Collection<? extends Message>) objectInputStream.readObject());
                    objectInputStream.close();
                    objectOutputStream.close();
                    socket.close();
                } catch (IOException e) {
                    Log.e("Client Query All Task", e.getStackTrace().toString());
                } catch (ClassNotFoundException e) {
                    Log.e("Client Query All Task", e.getStackTrace().toString());
                }

            }
            return messageList;
        }
    }

    class ServerTask extends AsyncTask<ServerSocket, Void, Void> {
        @Override
        protected Void doInBackground(ServerSocket... serverSockets) {
            ServerSocket serverSocket = serverSockets[0];

            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
                    try {
                        Message message = (Message) objectInputStream.readObject();
                        switch (message.getMessageStatus()) {
                            case INSERT:
                                insert(message);
                                socket.close();
                                break;
                            case QUERY:
                                writeBack(socket, database.query(message.getKey()));
                                break;
                            case DELETE:
                                delete(message);
                                socket.close();
                                break;
                            case QUERY_ALL:
                                writeBackAll(socket);
                                break;
                            case DELETE_ALL:
                                deleteAll();
                                socket.close();
                                break;
                            default:
                                break;
                        }
                    } catch (ClassNotFoundException e) {
                        Log.e("Server Task", e.getStackTrace().toString());
                    }
                } catch (IOException e) {
                    Log.e("Server Task", e.getStackTrace().toString());
                }
            }
        }

        private void deleteAll() {
            database.deleteAll();
        }

        private void writeBackAll(Socket socket) {
            ObjectOutputStream objectOutputStream = null;
            List<Message> messages = getListFromCursor(database.getAllMessages());
            try {
                objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                objectOutputStream.writeObject(messages);
                objectOutputStream.flush();
            } catch (IOException e) {
                Log.e("Write Back All", e.getStackTrace().toString());
            }
        }

        private void delete(Message message) {
            database.delete(message.getKey());
        }

        private void writeBack(Socket socket, Cursor query) {
            ObjectOutputStream objectOutputStream = null;
            Message message = new Message();
            if (query.moveToFirst()) {
                message.setKey(query.getString(0));
                message.setValue(query.getString(1));
            } else {
                message.setMessageStatus(MessageStatus.DNF);
            }
            try {
                objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                objectOutputStream.writeObject(message);
                objectOutputStream.flush();
            } catch (IOException e) {
                Log.e("Write Back", e.getStackTrace().toString());
            }
        }

    }

    public void insert(Message message) {
        ContentValues contentValues = new ContentValues();
        contentValues.put("key", message.getKey());
        contentValues.put("value", message.getValue());
        contentValues.put("version", message.getVersion());
        database.insert(contentValues);
    }
}
