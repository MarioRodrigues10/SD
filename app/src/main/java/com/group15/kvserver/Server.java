package com.group15.kvserver;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.group15.kvserver.utils.Logger;

import java.util.concurrent.locks.Condition;
import java.util.Set;

enum RequestType {
    AuthRequest((short)0),
    RegisterRequest((short)1),
    PutRequest((short)2),
    GetRequest((short)3),
    MultiPutRequest((short)4),
    MultiGetRequest((short)5),
    GetWhenRequest((short)6);

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
    Map<String, Condition> conditions;
    ReentrantLock globalLock = new ReentrantLock();

    public ServerDatabase(int databaseShardsCount, int usersShardsCount) {
        this.databaseShardsCount = databaseShardsCount;
        this.usersShardsCount = usersShardsCount;
        
        this.databaseShards = new java.util.ArrayList<>();
        this.usersShards = new java.util.ArrayList<>();

        this.databaseLocks = new java.util.ArrayList<>();
        this.usersLocks = new java.util.ArrayList<>();
        
        this.conditions = new HashMap<>(); 

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
                        Logger.log("Invalid request type: " + requestType, Logger.LogLevel.ERROR);
                    }
                }
                catch (EOFException e) {
                    // Client disconnects
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
        finally {
            Logger.log("Client disconnected.", Logger.LogLevel.INFO);
            Server.signalClientDisconnection();
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
                case GetWhenRequest:
                    return handleGetWhenRequest(in);
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

    private DataOutputStream handleGetWhenRequest(DataInputStream in) throws IOException {
        // Chaves e valores para a condição
        String key = in.readUTF();
        String keyCond = in.readUTF();
        int valueCondLength = in.readInt();
        byte[] valueCond = new byte[valueCondLength];
        in.readFully(valueCond);

        byte[] result = getWhen(key, keyCond, valueCond);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        if (result != null) {
            out.writeInt(result.length);
            out.write(result);
            return out;
        }
        else{
            out.writeInt(-1);
            return out;
        }
    }

    private void put(String key, byte[] value) {

        int shardIndex = database.getDatabaseShardIndex(key);
        database.databaseLocks.get(shardIndex).writeLock().lock();
        try {
            Map<String, byte[]> currentShard = database.databaseShards.get(shardIndex);
            currentShard.put(key, value);
            updateConditionAndNotify(key);
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
        Map<Integer, Map<String, byte[]>> pairsByShard = new java.util.HashMap<>();
        for (Map.Entry<String, byte[]> entry : pairs.entrySet()) {
            String key = entry.getKey();
            int shardIndex = database.getDatabaseShardIndex(key);
            if (!pairsByShard.containsKey(shardIndex)) {
                pairsByShard.put(shardIndex, new java.util.HashMap<>());
            }
            pairsByShard.get(shardIndex).put(key, entry.getValue());
        }

        database.globalLock.lock();
        try {
            for(Map.Entry<Integer, Map<String, byte[]>> shardPairs : pairsByShard.entrySet()) {
                int shardIndex = shardPairs.getKey();
                database.databaseLocks.get(shardIndex).writeLock().lock();
            }
        } finally {
            database.globalLock.unlock();
        }

        for(Map.Entry<Integer, Map<String, byte[]>> shardPairs : pairsByShard.entrySet()) {
            int shardIndex = shardPairs.getKey();
            Map<String, byte[]> currentShard = database.databaseShards.get(shardIndex);
            Map<String, byte[]> par = shardPairs.getValue();
            for (Map.Entry<String, byte[]> entry : par.entrySet()) {
                String key = entry.getKey();
                byte[] value = entry.getValue();
                currentShard.put(key, value);
                updateConditionAndNotify(key);
            }
            database.databaseLocks.get(shardIndex).writeLock().unlock();
        }
    }

    private Map<String, byte[]> multiGet(Set<String> keys) {
        Map<String, byte[]> pairs = new java.util.HashMap<>();
        Map<Integer, List<String>> keysByShard = new java.util.HashMap<>();
        for (String key : keys) {
            int shardIndex = database.getDatabaseShardIndex(key);
            if (!keysByShard.containsKey(shardIndex)) {
                keysByShard.put(shardIndex, new java.util.ArrayList<>());
            }
            keysByShard.get(shardIndex).add(key);
        }

        database.globalLock.lock();
        try{
            for (Map.Entry<Integer, List<String>> entry : keysByShard.entrySet()) {
                int shardIndex = entry.getKey();
                database.databaseLocks.get(shardIndex).readLock().lock();
            }
        }
        finally {
            database.globalLock.unlock();
        }

        for(Map.Entry<Integer, List<String>> shardKeys : keysByShard.entrySet()) {
            int shardIndex = shardKeys.getKey();
            List<String> keysByShardList = shardKeys.getValue();
            Map<String, byte[]> currentShard = database.databaseShards.get(shardIndex);
            for (String key : keysByShardList) {
                pairs.put(key, currentShard.get(key));
            }
            database.databaseLocks.get(shardIndex).readLock().unlock();
        }
        
        return pairs;
    }

    private byte[] getWhen(String key, String keyCond, byte[] valueCond) throws IOException {
        int shardIndexCond = database.getDatabaseShardIndex(keyCond);
        ReentrantReadWriteLock lock = database.databaseLocks.get(shardIndexCond);
        lock.writeLock().lock();
        try {
            Map<String, byte[]> currentShardCond = database.databaseShards.get(shardIndexCond);
            Condition condition = lock.writeLock().newCondition();
            database.conditions.put(keyCond, condition);

            while (!java.util.Arrays.equals(currentShardCond.get(keyCond), valueCond)) {
                try {
                    condition.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        } finally {
            lock.writeLock().unlock();
        }

        int shardIndex = database.getDatabaseShardIndex(key);
        ReentrantReadWriteLock targetLock = database.databaseLocks.get(shardIndex);
        targetLock.readLock().lock();
        try {
            Map<String, byte[]> currentShard = database.databaseShards.get(shardIndex);
            return currentShard.get(key);
        } finally {
            targetLock.readLock().unlock();
        }

    }

    private void updateConditionAndNotify(String keyCond) {
        int shardIndexCond = database.getDatabaseShardIndex(keyCond);
        ReentrantReadWriteLock lock = database.databaseLocks.get(shardIndexCond);
        lock.writeLock().lock();
        try {
            Map<String, byte[]> currentShardCond = database.databaseShards.get(shardIndexCond);

            if (currentShardCond.containsKey(keyCond)) {
                Condition condition = database.conditions.get(keyCond);
                Logger.log("Notifying condition for key: " + keyCond, Logger.LogLevel.INFO);
                if (condition != null) {
                    condition.signalAll();
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}

public class Server {
    static int connectedClients = 0;
    static ReentrantLock lock = new ReentrantLock();
    static Condition allowClientConnection = lock.newCondition();

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

        int maxClients = arguments.get(0);
        ServerDatabase database = new ServerDatabase(arguments.get(1), arguments.get(2));
        ServerSocket serverSocket = new ServerSocket(12345);

        Logger.log("Server started. Listening on port 12345", Logger.LogLevel.INFO);
        // log maxClients, databaseShards, userShards
        Logger.log("Max clients: " + maxClients + ", Database shards: " + arguments.get(1), Logger.LogLevel.INFO);

        boolean running = true;
        while (running) {
            lock.lock();
            try {
                while (connectedClients >= maxClients) {
                    allowClientConnection.await();
                }

                Socket socket = serverSocket.accept();
                connectedClients++;
                Logger.log("Client connected. Active clients: " + connectedClients, Logger.LogLevel.INFO);
                Thread worker = new Thread(new ServerWorker(socket, database));
                worker.start();

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {lock.unlock();}
        }
    }

    public static void signalClientDisconnection(){
        lock.lock();
        try{
            connectedClients--;
            allowClientConnection.signalAll();
            Logger.log("Client disconnected. Active clients: " + connectedClients, Logger.LogLevel.INFO);
        }
        finally {
            lock.unlock();
        }
    }

}
