package com.group15.kvserver;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The TaggedConnection class represents a connection over a socket with the ability to send and receive tagged frames.
 * Each frame consists of a tag, a request type, and a data payload. The connection ensures thread-safe communication
 * using locks for both sending and receiving frames.
 * 
 * This class implements AutoCloseable to handle the closing of the socket connection properly when it is no longer needed.
 */
public class TaggedConnection implements AutoCloseable {
    /* The socket associated with this connection */
    private final Socket socket;
    /* The input stream for reading data from the socket */
    private final DataInputStream in;
    /* The output stream for writing data to the socket */
    private final DataOutputStream out;
    /* Lock for sending frames */
    private final Lock sendLock = new ReentrantLock();
    /* Lock for receiving frames */
    private final Lock receiveLock = new ReentrantLock();

    /**
     * A nested class representing a frame of data that can be sent or received over the connection.
     * Each frame contains a tag, a request type, and the data payload.
     */
    public static class Frame {
        /* The tag that identifies the frame. */
        public final int tag;
        /* The request type of the frame. */
        public final short requestType;
        /* The data payload of the frame. */
        public final byte[] data;

        /**
         * Constructs a new Frame with the given tag, request type, and data payload.
         * 
         * @param tag The tag identifying the frame.
         * @param requestType The request type of the frame.
         * @param data The data payload of the frame.
         */
        public Frame(int tag, short requestType ,byte[] data) {
            this.tag = tag;
            this.requestType = requestType;
            this.data = data;
        }
    }

    /**
     * Constructs a TaggedConnection with the given socket.
     * Initializes the input and output streams for communication over the socket.
     * 
     * @param socket The socket used for the connection.
     * @throws IOException If an error occurs while creating the input/output streams.
     */
    public TaggedConnection(Socket socket) throws IOException {
        this.socket = socket;
        this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
    }

    /**
     * Sends a frame over the connection. The frame consists of a tag, request type, and data.
     * 
     * @param frame The frame to be sent.
     * @throws IOException If an error occurs during sending the frame.
     */
    public void send(Frame frame) throws IOException {
        send(frame.tag, frame.requestType ,frame.data);
    }

    /**
     * Sends a frame over the connection with the specified tag, request type, and data.
     * Ensures thread-safety by locking the send operation.
     * 
     * @param tag The tag for the frame.
     * @param request The request type for the frame.
     * @param data The data to be sent.
     * @throws IOException If an error occurs during sending the frame.
     */
    public void send(int tag, short request, byte[] data) throws IOException {
        sendLock.lock();
        try {
            out.writeInt(tag); 
            out.writeShort(request);
            out.writeInt(data.length); 
            out.write(data); 
            out.flush();
        } finally {
            sendLock.unlock();
        }
    }

    /**
     * Receives a frame from the connection. The method blocks until a complete frame is received.
     * Ensures thread-safety by locking the receive operation.
     * 
     * @return A Frame object containing the received tag, request type, and data.
     * @throws IOException If an error occurs during receiving the frame.
     */
    public Frame receive() throws IOException {
        receiveLock.lock();
        try {
            int tag = in.readInt(); 
            short request = in.readShort();
            int length = in.readInt(); 
            byte[] data = new byte[length];
            in.readFully(data); 
            return new Frame(tag, request, data);
        } finally {
            receiveLock.unlock();
        }
    }

    /**
     * Closes the connection by closing the underlying socket.
     * 
     * @throws IOException If an error occurs while closing the socket.
     */
    @Override
    public void close() throws IOException {
        socket.close();
    }
}