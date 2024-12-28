package com.group15.kvserver;

import java.io.EOFException;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import com.group15.kvserver.utils.Logger;

/**
 * The Demultiplexer class is responsible for managing incoming and outgoing messages
 * over a tagged connection. It decouples the reception of messages into multiple queues
 * based on their tags, allowing different components to handle messages independently.
 * 
 * This class implements AutoCloseable to ensure proper resource management (such as closing
 * connections and stopping threads when no longer needed).
 */
public class Demultiplexer implements AutoCloseable {
    /* The connection used to send/receive messages. */
    private final TaggedConnection conn;
    /* A map of queues for storing incoming messages based on their tags. */
    private final Map<Integer, BlockingQueue<byte[]>> queues = new ConcurrentHashMap<>();
    /* A thread for reading incoming messages from the connection. */
    private final Thread readerThread;
    /* A flag indicating whether the demultiplexer is closed. */
    private volatile boolean closed = false;
    /* The client library used to handle responses. */
    private ClientLibrary clientLibrary = null;

    /**
     * Constructs a Demultiplexer with the given TaggedConnection.
     * Initializes the reader thread to handle message reception.
     * 
     * @param conn The tagged connection used for communication.
     */
    public Demultiplexer(TaggedConnection conn) {
        this.conn = conn;
        this.readerThread = new Thread(this::reader);
        this.readerThread.start();
    }

    /**
     * Sets the ClientLibrary instance to interact with the client application.
     * 
     * @param clientLibrary The client library to be set.
     */
    public void setClientLibrary(ClientLibrary clientLibrary) {
        this.clientLibrary = clientLibrary;
    }

    /**
     * The reader method that continuously listens for incoming messages on the connection.
     * When a message is received, it is added to the corresponding queue based on the tag
     * and forwarded to the client library if necessary.
     */
    public void reader() {
        try {
            while (!closed) {
                try{
                    TaggedConnection.Frame frame = conn.receive();
                    BlockingQueue<byte[]> queue = queues.computeIfAbsent(frame.tag, k -> new ArrayBlockingQueue<>(1024));
                    queue.put(frame.data);

                    if (clientLibrary != null) clientLibrary.addResponse(frame.tag, frame.data);
                } catch (EOFException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        } catch (IOException | InterruptedException e) {
            if (!closed) {
                e.printStackTrace();
            }
        }
        finally{
            queues.values().forEach(q -> q.clear());
        }
    }

    /**
     * Starts the reader thread if it is not already running.
     */
    public void start() {
        if (!readerThread.isAlive()) {
            readerThread.start();
        }
    }

    /**
     * Sends a TaggedConnection frame over the connection.
     * 
     * @param frame The frame to be sent.
     * @throws IOException If an error occurs during sending.
     */
    public void send(TaggedConnection.Frame frame) throws IOException {
        conn.send(frame);
    }

    /**
     * Sends a tagged frame with a specific request type and data.
     * 
     * @param tag The tag associated with the request.
     * @param request The request type (short).
     * @param data The data to be sent.
     * @throws IOException If an error occurs during sending.
     */
    public void send(int tag, short request, byte[] data) throws IOException {
        conn.send(new TaggedConnection.Frame(tag, request, data));
    }

    /**
     * Receives data associated with a specific tag from the demultiplexer.
     * Blocks until the data is available.
     * 
     * @param tag The tag to receive data for.
     * @return The data associated with the tag.
     * @throws IOException If an error occurs during reception.
     * @throws InterruptedException If the thread is interrupted while waiting.
     */
    public byte[] receive(int tag) throws IOException, InterruptedException {
        BlockingQueue<byte[]> queue = queues.computeIfAbsent(tag, k -> new ArrayBlockingQueue<>(1024));
        return queue.take();
    }

    /**
     * Receives any data from the demultiplexer, regardless of the tag.
     * Blocks until data is available.
     * 
     * @return A frame containing the tag and data received.
     * @throws InterruptedException If the thread is interrupted while waiting.
     */
    public TaggedConnection.Frame receiveAny() throws InterruptedException {
        while (!closed) {
            for (Map.Entry<Integer, BlockingQueue<byte[]>> entry : queues.entrySet()) {
                BlockingQueue<byte[]> queue = entry.getValue();
                byte[] data = queue.poll();
    
                if (data != null) {
                    return new TaggedConnection.Frame(entry.getKey(), (short) 0, data);
                }
            }
        }
        throw new InterruptedException("Demultiplexer is closed.");
    }  

    /**
     * Closes the demultiplexer, stopping the reader thread and closing the connection.
     * 
     * @throws IOException If an error occurs while closing the connection.
     */
    @Override
    public void close() throws IOException {
        closed = true;
        readerThread.interrupt();
        conn.close();
        Logger.log("Connection closed successfully.", Logger.LogLevel.INFO);
    }
}