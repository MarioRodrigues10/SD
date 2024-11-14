package com.group15.kvserver;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.Set;

enum RequestType {
    AuthRequest((short)0),
    RegisterRequest((short)1),
    PutRequest((short)2),
    GetRequest((short)3),
    MultiPutRequest((short)4),
    MultiGetRequest((short)5);

    private final short value;

    RequestType(short value) {
        this.value = value;
    }

    public short getValue() {
        return value;
    }
}

class ServerDatabase {
    Map<String, byte[]> database;

    // key: username, value: password
    Map<String, String> users;

    public ServerDatabase() {
        this.database = new java.util.HashMap<>();
        this.users = new java.util.HashMap<>();
    }
}

class ServerWorker implements Runnable {
    private Socket socket;
    private ServerDatabase database;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public ServerWorker(Socket socket, ServerDatabase database) {
        this.socket = socket;
        this.database = database;
    }

    @Override
    public void run() {
        try {
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));

            boolean running = true;
            while (running) {
                try {
                    short requestType = in.readShort();
                    if (requestType >= 0 && requestType < RequestType.values().length) {
                        RequestType r = RequestType.values()[requestType];
                        DataOutputStream stream = handleRequest(r, in);

                        if (stream != null) {
                            stream.flush();
                        }
                    } else {
                        System.out.println("Error -> requestType: " + requestType);
                    }
                }
                catch (EOFException e) {
                    // Client disconnects
                    System.out.println("Client disconnected.");
                    running = false;
                }
            }

            socket.shutdownInput();
            socket.shutdownOutput();
            socket.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public DataOutputStream handleRequest(RequestType requestType, DataInputStream in){

        switch (requestType) {
            case AuthRequest:
                return handleAuthRequest(in);
            case RegisterRequest:
                return handleRegisterRequest(in);
            case PutRequest:
                return handlePutRequest(in);
            case GetRequest:
                return handleGetRequest(in);
            case MultiPutRequest:
                return handleMultiPutRequest(in);
            case MultiGetRequest:
                return handleMultiGetRequest(in);
            default:
                return null;
        }
    }

    private DataOutputStream handleAuthRequest(DataInputStream in) {
        lock.readLock().lock(); 
        try {
            String username = in.readUTF();
            String password = in.readUTF();

            if (database.users.containsKey(username) && database.users.get(username).equals(password)) {
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                out.writeBoolean(true);
                return out;
            } else {
                return null;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            lock.readLock().unlock(); 
        }
    }

    private DataOutputStream handleRegisterRequest(DataInputStream in) {
        lock.writeLock().lock();
        try {
            String username = in.readUTF();
            String password = in.readUTF();

            if (database.users.containsKey(username)) {
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                out.writeBoolean(false);
                return out;
            } else {
                database.users.put(username, password);

                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                out.writeBoolean(true);
                return out;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private DataOutputStream handlePutRequest(DataInputStream in){
        try {
            String key = in.readUTF();
            int valueLength = in.readInt();
            byte[] value = new byte[valueLength];
            in.readFully(value);

            put(key, value);

            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private DataOutputStream handleGetRequest(DataInputStream in){
        try {
            // KEY
            String key = in.readUTF();
            byte[] value = get(key);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            // VALUE SIZE | VALUE
            out.writeInt(value.length);
            out.write(value);
            return out;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private DataOutputStream handleMultiPutRequest(DataInputStream in) {
        try {
            // N PAIRS | KEY | VALUE LENGTH | VALUE | KEY | VALUE LENGTH | VALUE | ...
            int numberOfPairs = in.readInt();
            Map<String, byte[]> pairs = new java.util.HashMap<>();
    
            for (int i = 0; i < numberOfPairs; i++) {
                String key = in.readUTF();
                int valueLength = in.readInt();
                byte[] value = new byte[valueLength];
                in.readFully(value);
                pairs.put(key, value);
            }
    
            multiPut(pairs);

            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private DataOutputStream handleMultiGetRequest(DataInputStream in) {
        try {
            // N KEYS | KEY | ...
            int numberOfKeys = in.readInt();
            Set<String> keys = new java.util.HashSet<>();
            for (int i = 0; i < numberOfKeys; i++) {
                String key = in.readUTF();
                keys.add(key);
            }
            Map<String, byte[]> pairs = multiGet(keys);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            // N PAIRS | KEY | VALUE LENGTH | VALUE ..
            out.writeInt(numberOfKeys);
            for (Map.Entry<String, byte[]> pair : pairs.entrySet()) {
                out.writeUTF(pair.getKey());
                byte[] value = pair.getValue();
                out.writeInt(value.length);
                out.write(value);
            }

            return out;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private void put(String key, byte[] value) {
        lock.writeLock().lock();
        try {
            database.database.put(key, value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private byte[] get(String key) {
        lock.readLock().lock();
        try {
            return database.database.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    private void multiPut(Map<String, byte[]> pairs) {
        for (Map.Entry<String, byte[]> entry : pairs.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    private Map<String, byte[]> multiGet(Set<String> keys) {
        Map<String, byte[]> pairs = new HashMap<>();
        for (String key : keys) {
            byte[] value = get(key);
            if (value != null) {
                pairs.put(key, value);
            }
        }
        return pairs;
    }
}

public class Server {
    public static void main(String[] args) throws IOException {
        ServerDatabase database = new ServerDatabase(); 
        ServerSocket serverSocket = new ServerSocket(12345);

        boolean running = true;
        while (running) {
            Socket socket = serverSocket.accept();
            Thread worker = new Thread(new ServerWorker(socket, database));
            worker.start();
        }
    }
}
