package com.group15.kvserver;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
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
    int databaseShardsCount;
    int usersShardsCount;

    List<Map<String, byte[]>> databaseShards;
    List<Map<String, String>> usersShards;

    List<ReentrantReadWriteLock> databaseLocks;
    List<ReentrantLock> usersLocks;

    public ServerDatabase(int maxClients, int databaseShardsCount, int usersShardsCount) {
        this.databaseShardsCount = databaseShardsCount;
        this.usersShardsCount = usersShardsCount;
        
        this.databaseShards = new java.util.ArrayList<>();
        this.usersShards = new java.util.ArrayList<>();

        this.databaseLocks = new java.util.ArrayList<>();
        this.usersLocks = new java.util.ArrayList<>();

        for (int i = 0; i < databaseShardsCount; i++) {
            this.databaseShards.add(new HashMap<>());
            this.databaseLocks.add(new ReentrantReadWriteLock());
        }

        for (int i = 0; i < usersShardsCount; i++) {
            this.usersShards.add(new HashMap<>());
            this.usersLocks.add(new ReentrantLock());
        }
    }

    public int getDatabaseShardIndex(String key) {
        return Math.abs(key.hashCode()) % databaseShardsCount;
    }

    public int getUsersShardIndex(String key) {
        return Math.abs(key.hashCode()) % usersShardsCount;
    }
}

class ServerWorker implements Runnable {
    private Socket socket;
    private ServerDatabase database;

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
        try {
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
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private DataOutputStream handleAuthRequest(DataInputStream in) throws IOException {
        String username = in.readUTF();
        String password = in.readUTF();
        int userShardIndex = database.getUsersShardIndex(username);
        database.usersLocks.get(userShardIndex).lock();
        try {
            Map<String, String> currentShard = database.usersShards.get(userShardIndex);
            if (currentShard.containsKey(username) && currentShard.get(username).equals(password)) {
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                out.writeBoolean(true);
                return out;
            } else {
                return null;
            }
        } 
        finally {
            database.usersLocks.get(userShardIndex).unlock();
        }
    }

    private DataOutputStream handleRegisterRequest(DataInputStream in) throws IOException {
        String username = in.readUTF();
        String password = in.readUTF();
        int userShardIndex = database.getUsersShardIndex(username);
        database.usersLocks.get(userShardIndex).lock();
        try {
            Map<String, String> currentShard = database.usersShards.get(userShardIndex);
            if (currentShard.containsKey(username)) {
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                out.writeBoolean(false);
                return out;
            } else {
                currentShard.put(username, password);

                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                out.writeBoolean(true);
                return out;
            }
        } finally {
            database.usersLocks.get(userShardIndex).unlock();
        }
    }

    private DataOutputStream handlePutRequest(DataInputStream in) throws IOException{
        String key = in.readUTF();
        int valueLength = in.readInt();
        byte[] value = new byte[valueLength];
        in.readFully(value);

        put(key, value);

        return null;
    }

    private DataOutputStream handleGetRequest(DataInputStream in) throws IOException{
        // KEY
        String key = in.readUTF();
        byte[] value = get(key);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());

        // VALUE SIZE | VALUE
        out.writeInt(value.length);
        out.write(value);
        return out;
    }

    private DataOutputStream handleMultiPutRequest(DataInputStream in) throws IOException {
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
    }

    private DataOutputStream handleMultiGetRequest(DataInputStream in) throws IOException {
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
    }

    private void put(String key, byte[] value) {

        int shardIndex = database.getDatabaseShardIndex(key);
        database.databaseLocks.get(shardIndex).writeLock().lock();
        try {
            Map<String, byte[]> currentShard = database.databaseShards.get(shardIndex);
            currentShard.put(key, value);
        } finally {
            database.databaseLocks.get(shardIndex).writeLock().unlock();
        }
    }

    private byte[] get(String key) {
        int shardIndex = database.getDatabaseShardIndex(key);
        database.databaseLocks.get(shardIndex).readLock().lock();
        try {
            Map<String, byte[]> currentShard = database.databaseShards.get(shardIndex);
            return currentShard.get(key);
        } finally {
            database.databaseLocks.get(shardIndex).readLock().unlock();
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
        List<Integer> arguments = new java.util.ArrayList<>();

        if(args.length == 3) {
            for(String arg : args) {
                try {
                    arguments.add(Integer.parseInt(arg));
                } catch (NumberFormatException e) {
                    System.out.println("Usage: java Server <max-clients> <database-shards> <user-shards>");
                    return;
                }
            }
        } else {
            System.out.println("Usage: java Server <max-clients> <database-shards> <user-shards>");
            return;
        }

        ServerDatabase database = new ServerDatabase(arguments.get(0), arguments.get(1), arguments.get(2)); 
        ServerSocket serverSocket = new ServerSocket(12345);

        boolean running = true;
        while (running) {
            Socket socket = serverSocket.accept();
            Thread worker = new Thread(new ServerWorker(socket, database));
            worker.start();
        }
    }
}
