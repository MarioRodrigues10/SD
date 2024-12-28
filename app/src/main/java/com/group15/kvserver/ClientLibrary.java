package com.group15.kvserver;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


public class ClientLibrary {
    //private Socket socket;
    //private DataOutputStream out;
    //private DataInputStream in;
    private TaggedConnection taggedConnection;
    private Demultiplexer demultiplexer;
    private final ReentrantLock lock = new ReentrantLock();
    private int tag = 0;

    public Map<Integer, Condition> conditionsMap = new HashMap<>();
    public Map<Integer, byte[]> responsesMap = new HashMap<>();

    public ClientLibrary(String host, int port) throws IOException {
        /*this.socket = new Socket(host, port);
        this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        */
        Socket socket = new Socket(host, port);
        taggedConnection = new TaggedConnection(socket);
        demultiplexer = new Demultiplexer(taggedConnection);

        demultiplexer.setClientLibrary(this);
        new Thread(demultiplexer::reader).start();
    }

    private byte[] sendWithTag(short requestType, byte[] requestData) throws IOException {
        lock.lock();
        try {
            TaggedConnection.Frame frame = new TaggedConnection.Frame(this.tag, requestType, requestData);
            this.tag++;
            taggedConnection.send(frame.tag, requestType, requestData);
            
            try {
                return demultiplexer.receive(frame.tag);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for response", e);
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean authenticate(String username, String password) throws IOException {
        lock.lock();
        try {
            // Envia um pedido de autenticação com as credenciais
            byte[] requestData;
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)) {
                dos.writeShort(RequestType.AuthRequest.getValue());
                dos.writeUTF(username);
                dos.writeUTF(password);
                requestData = baos.toByteArray();
            }
            System.out.println("Sending authentication request");
            byte[] responseData = sendWithTag(RequestType.AuthRequest.getValue(), requestData);
            // Lê a resposta
            try (ByteArrayInputStream bais = new ByteArrayInputStream(responseData);
                 DataInputStream dis = new DataInputStream(bais)) {
                return dis.readBoolean();
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean register(String username, String password) throws IOException {
        lock.lock();
        try {
            byte[] requestData;
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)) {
                dos.writeShort(RequestType.RegisterRequest.getValue());
                dos.writeUTF(username);
                dos.writeUTF(password);
                requestData = baos.toByteArray();
            }
            byte[] responseData = sendWithTag(RequestType.RegisterRequest.getValue(), requestData);
            // Lê a resposta
            try (ByteArrayInputStream bais = new ByteArrayInputStream(responseData);
                 DataInputStream dis = new DataInputStream(bais)) {
                return dis.readBoolean();
            }
        } finally {
            lock.unlock();
        }
    }

    public void put(String key, byte[] value) throws IOException {
        lock.lock();
        try {
            // Envia um pedido de inserção com a chave e o valor
            byte[] requestData;
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)) {
                dos.writeShort(RequestType.PutRequest.getValue());
                dos.writeUTF(key);
                dos.writeInt(value.length);
                dos.write(value);
                requestData = baos.toByteArray();
            }
            System.out.println("Sending put request for key: " + key);
            sendWithTag(RequestType.PutRequest.getValue(), requestData);
        } finally {
            lock.unlock();
        }
    }

    public byte[] get(String key) throws IOException {
        lock.lock();
        try {
            int tagG = this.tag;
            this.tag++;
            byte[] requestData;
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)) {
                dos.writeShort(RequestType.GetRequest.getValue());
                dos.writeUTF(key);
                requestData = baos.toByteArray();
            }

            Condition condition = lock.newCondition();
            conditionsMap.put(tagG, condition);

            demultiplexer.send(tagG, RequestType.GetRequest.getValue(), requestData);
            
            while (!responsesMap.containsKey(tagG)) {
                try {
                    condition.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting for response", e);
                }
            }

            byte[] response = responsesMap.remove(tagG);
            conditionsMap.remove(tagG);

            try (ByteArrayInputStream bais = new ByteArrayInputStream(response);
                DataInputStream dis = new DataInputStream(bais)) {
                int length = dis.readInt();
                if (length < 0) return null;
                byte[] data = new byte[length];
                dis.readFully(data);
                return data;
            }
        } finally {
            lock.unlock();
        }
    }

    public void multiPut(Map<String, byte[]> pairs) throws IOException {
        lock.lock();
        try {
            // Envia um pedido de inserção múltipla com os pares chave-valor
            byte[] requestData;
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)) {
                dos.writeShort(RequestType.MultiPutRequest.getValue());
                dos.writeInt(pairs.size());
                for (Map.Entry<String, byte[]> entry : pairs.entrySet()) {
                    dos.writeUTF(entry.getKey());
                    dos.writeInt(entry.getValue().length);
                    dos.write(entry.getValue());
                }
                requestData = baos.toByteArray();
            }
            sendWithTag(RequestType.MultiPutRequest.getValue(), requestData);
        } finally {
            lock.unlock();
        }
    }

    public Map<String, byte[]> multiGet(Set<String> keys) throws IOException {
        lock.lock();
        try {
            int tagG = this.tag;
            this.tag++;
            byte[] requestData;
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)) {
                dos.writeShort(RequestType.MultiGetRequest.getValue());
                dos.writeInt(keys.size());
                for (String key : keys) {
                    dos.writeUTF(key);
                }
                requestData = baos.toByteArray();
            }

            Condition condition = lock.newCondition();
            conditionsMap.put(tagG, condition);

            demultiplexer.send(tagG, RequestType.MultiGetRequest.getValue(), requestData);

            while (!responsesMap.containsKey(tagG)) {
                try {
                    condition.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting for response", e);
                }
            }

            byte[] responseData = responsesMap.remove(tagG);
            conditionsMap.remove(tagG);
            
            try (ByteArrayInputStream bais = new ByteArrayInputStream(responseData);
                 DataInputStream dis = new DataInputStream(bais)) {
                int n = dis.readInt();
                Map<String, byte[]> result = new HashMap<>();
                for (int i = 0; i < n; i++) {
                    String key = dis.readUTF();
                    int length = dis.readInt();
                    byte[] data = new byte[length];
                    dis.readFully(data);
                    result.put(key, data);
                }
                return result;
            }
        } finally {
            lock.unlock();
        }
    }

    public byte[] getWhen(String key, String keyCond, byte[] valueCond) throws IOException, InterruptedException {
        lock.lock();
        try {
            int tagG = this.tag;
            this.tag++;
            byte[] requestData;
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 DataOutputStream dos = new DataOutputStream(baos)) {
                dos.writeShort(RequestType.GetWhenRequest.getValue());
                dos.writeUTF(key);
                dos.writeUTF(keyCond);
                dos.writeInt(valueCond.length);
                dos.write(valueCond);
                requestData = baos.toByteArray();
            }

            Condition condition = lock.newCondition();
            conditionsMap.put(tagG, condition);

            demultiplexer.send(tagG, RequestType.GetWhenRequest.getValue(), requestData);
    
            while (!responsesMap.containsKey(tagG)) {
                try {
                    condition.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting for response", e);
                }
            }

            byte[] response = responsesMap.remove(tagG);
            conditionsMap.remove(tagG);
            return response;

        } finally {
            lock.unlock();
        }
    }

    public void addResponse(int tagR, byte[] response) {
        lock.lock();
        try {
            if (conditionsMap.containsKey(tagR) || (tagR == 3)) {
                responsesMap.put(tagR, response);
                conditionsMap.get(tagR).signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    public void close() throws IOException {
        lock.lock();
        try {
            sendDisconnectMessage();
            demultiplexer.close();
        } finally {
            lock.unlock();
        }
    }

    public void sendDisconnectMessage() throws IOException {
        lock.lock();
        try {
            byte[] disconnect;
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 DataOutputStream dos = new DataOutputStream(baos)) {
                dos.writeShort(RequestType.DisconnectRequest.getValue());
                disconnect = baos.toByteArray();
            }
            sendWithTag(RequestType.DisconnectRequest.getValue(), disconnect);
        } finally {
            lock.unlock();
        }
    }
    
}
