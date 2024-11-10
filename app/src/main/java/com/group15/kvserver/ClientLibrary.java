package com.group15.kvserver;

import java.io.*;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ClientLibrary {
    private Socket socket;
    private DataOutputStream out;
    private DataInputStream in;

    public ClientLibrary(String host, int port) throws IOException {
        this.socket = new Socket(host, port);
        this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
    }

    public boolean authenticate(String username, String password) throws IOException {
        out.writeShort(RequestType.AuthRequest.getValue());
        out.writeUTF(username);
        out.writeUTF(password);
        out.flush();
        return in.readBoolean();
    }

    public boolean register(String username, String password) throws IOException {
        out.writeShort(RequestType.RegisterRequest.getValue());
        out.writeUTF(username);
        out.writeUTF(password);
        out.flush();
        return in.readBoolean();
    }

    public void put(String key, byte[] value) throws IOException {
        out.writeShort(RequestType.PutRequest.getValue());
        out.writeUTF(key);
        out.writeInt(value.length);
        out.write(value);
        out.flush();
    }

    public byte[] get(String key) throws IOException {
        out.writeShort(RequestType.GetRequest.getValue());
        out.writeUTF(key);
        out.flush();
        int length = in.readInt();
        if (length < 0) return null;
        byte[] data = new byte[length];
        in.readFully(data);
        return data;
    }

    public void multiPut(Map<String, byte[]> pairs) throws IOException {
        out.writeShort(RequestType.MultiPutRequest.getValue());
        out.writeInt(pairs.size());
        for (Map.Entry<String, byte[]> entry : pairs.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeInt(entry.getValue().length);
            out.write(entry.getValue());
        }
        out.flush();
    }

    public Map<String, byte[]> multiGet(Set<String> keys) throws IOException {
        out.writeShort(RequestType.MultiGetRequest.getValue());
        out.writeInt(keys.size());
        for (String key : keys) {out.writeUTF(key);}
        out.flush();

        int length = in.readInt();
        if (length < 0) return null;
        Map<String, byte[]> result = new HashMap<>();
        for (int i = 0; i < length; i++) {
            String key = in.readUTF();
            int valueLen = in.readInt();
            byte[] value = new byte[valueLen];
            in.readFully(value);
            result.put(key, value);
        }
        return result;
    }

    public void close() throws IOException {
        in.close();
        out.close();
        socket.close();
    }
}
