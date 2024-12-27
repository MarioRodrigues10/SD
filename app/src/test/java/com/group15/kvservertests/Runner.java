package com.group15.kvservertests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.title.TextTitle;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import com.group15.kvserver.ClientLibrary;
import com.group15.kvserver.utils.Logger;

public class Runner {
    private static final String HOST = "localhost";
    private static final int PORT = 12345;
    public int numBuckets;
    public int maxClients;

    public static void main(String[] args) {
        try {
            Runner runner = new Runner();
            boolean result = runner.init();

            if (!result) {
                Logger.log("Can't initialize test environment. Server needs to be active!", Logger.LogLevel.ERROR);
                return;
            }

            Scanner scanner = new Scanner(System.in);
            System.out.print("Enter the number of shards the server has:\n|> ");
            runner.numBuckets = scanner.nextInt();

            System.out.print("Enter the maximum number of clients the server can handle at the same time:\n|> ");
            runner.maxClients = scanner.nextInt();

            System.out.println("Select workload to run:");
            System.out.println("1. Get 1000 random key value pairs");
            System.out.println("2. Get 1000 operations on the same key (testing hotspot behaviour)");
            System.out.println("3. MultiGet 1000 random key value pairs");
            System.out.println("4. MultiGet 100 operations on the same key (10 keys per multiget)");
            System.out.print("|> ");
            int workload = scanner.nextInt();

            if (workload == 1) {
                runner.workload1();
            } else if (workload == 2) {
                runner.workload2();
            } else if (workload == 3) {
                runner.workload3();
            } else if (workload == 4) {
                runner.workload4();
            } else {
                Logger.log("Invalid workload selected.", Logger.LogLevel.ERROR);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean init() throws IOException {
        try {
            ClientLibrary client = new ClientLibrary(HOST, PORT);
            client.close();
            return true;
        } catch (Exception e) {
            Logger.log("Initialization failed: " + e.getMessage(), Logger.LogLevel.ERROR);
            return false;
        }
    }

    public void workload1() throws IOException {
        Logger.log("Running workload 1", Logger.LogLevel.INFO);

        ExecutorService executorService = Executors.newFixedThreadPool(maxClients);
        List<Long> responseTimes = new ArrayList<>();
        List<Long> timestamps = new ArrayList<>();
        ReentrantLock datapointsLock = new ReentrantLock();
        
        Logger.log("Populating database.", Logger.LogLevel.INFO);
        for (int i = 1; i <= 1000; i++) {
            int finalI = i;
            Thread put = new Thread(() -> {
                try {
                    put("key" + finalI, ("value" + finalI).getBytes());
                } catch (IOException e) {
                    System.out.println("Error during put operation: " + e.getMessage());
                }
            });
            put.start();
        }

        for (int i = 1; i <= 1000; i++) {
            final String key = "key" + i;
            final String value = "value" + i;
            final long workloadStartTime = System.currentTimeMillis();
            executorService.submit(() -> {
                try {
                    Thread getThread = new Thread(() -> {
                        try {
                            long startTime = System.nanoTime();
                            byte[] returnedValue = get(key);
                            long endTime = System.nanoTime();
                            long duration = endTime - startTime;
                            long timestamp = System.currentTimeMillis() - workloadStartTime;
    
                            if (returnedValue != null) {
                                if (!value.equals(new String(returnedValue))) {
                                    Logger.log("Value mismatch for key " + key, Logger.LogLevel.ERROR);
                                }
                            }
    
                            datapointsLock.lock();
                            try {
                                responseTimes.add(duration);
                                timestamps.add(timestamp);
                            } finally {
                                datapointsLock.unlock();
                            }
                        } catch (IOException e) {
                            Logger.log("Get failed for key " + key + ": " + e.getMessage(), Logger.LogLevel.ERROR);
                        }
                    });
    
                    getThread.start();
                } catch (Exception e) {
                    Logger.log("Error during get operation for key " + key + ": " + e.getMessage(), Logger.LogLevel.ERROR);
                }
            });
        }

        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
    
        generateGraph(responseTimes, timestamps, "Get Operation Response time over time for a Server with " + numBuckets + " bucket(s) and " + maxClients + " client(s)", "1000 random key value pairs");
    }

    public void workload2() throws IOException {
        Logger.log("Running workload 2", Logger.LogLevel.INFO);

        ExecutorService executorService = Executors.newFixedThreadPool(maxClients);
        List<Long> responseTimes = new ArrayList<>();
        List<Long> timestamps = new ArrayList<>();
        ReentrantLock datapointsLock = new ReentrantLock();
        
        Logger.log("Populating database.", Logger.LogLevel.INFO);

        Thread put = new Thread(() -> {
            try {
                put("key", ("value").getBytes());
            } catch (IOException e) {
                System.out.println("Error during put operation: " + e.getMessage());
            }
        });
        put.start();

        for (int i = 1; i <= 1000; i++) {
            final String key = "key";
            final String value = "value";
            final long workloadStartTime = System.currentTimeMillis();
            executorService.submit(() -> {
                try {
                    Thread getThread = new Thread(() -> {
                        try {
                            long startTime = System.nanoTime();
                            byte[] returnedValue = get(key);
                            long endTime = System.nanoTime();
                            long duration = endTime - startTime;
                            long timestamp = System.currentTimeMillis() - workloadStartTime;
    
                            if (returnedValue != null) {
                                if (!value.equals(new String(returnedValue))) {
                                    Logger.log("Value mismatch for key " + key, Logger.LogLevel.ERROR);
                                }
                            }
    
                            datapointsLock.lock();
                            try {
                                responseTimes.add(duration);
                                timestamps.add(timestamp);
                            } finally {
                                datapointsLock.unlock();
                            }
                        } catch (IOException e) {
                            Logger.log("Get failed for key " + key + ": " + e.getMessage(), Logger.LogLevel.ERROR);
                        }
                    });
    
                    getThread.start();
                } catch (Exception e) {
                    Logger.log("Error during get operation for key " + key + ": " + e.getMessage(), Logger.LogLevel.ERROR);
                }
            });
        }

        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }

        generateGraph(responseTimes, timestamps, "Get Operation Response time over time for a Server with " + numBuckets + " bucket(s) and " + maxClients + " client(s)", "1000 get operations on the same key (testing hotspot behaviour)");
    }

    public void workload3() throws IOException {
        Logger.log("Running workload 3", Logger.LogLevel.INFO);

        ExecutorService executorService = Executors.newFixedThreadPool(maxClients);
        List<Long> responseTimes = new ArrayList<>();
        List<Long> timestamps = new ArrayList<>();
        ReentrantLock datapointsLock = new ReentrantLock();
        
        Logger.log("Populating database.", Logger.LogLevel.INFO);
        for (int i = 1; i <= 1000; i++) {
            int finalI = i;
            Thread put = new Thread(() -> {
                try {
                    put("key" + finalI, ("value" + finalI).getBytes());
                } catch (IOException e) {
                    System.out.println("Error during put operation: " + e.getMessage());
                }
            });
            put.start();
        }

        for (int i = 1; i <= 100; i++) {
            final int currentIteration = i;
            final long workloadStartTime = System.currentTimeMillis();
            executorService.submit(() -> {
                try {
                    Thread multiGetThread = new Thread(() -> {
                        try {
                            Set<String> keys = new HashSet<>();
                            for (int j = 1; j <= 10; j++) {
                                keys.add("key" + (currentIteration * j));
                            }
    
                            long startTime = System.nanoTime();
                            Map<String, byte[]> returnedValues = multiGet(keys);
                            long endTime = System.nanoTime();
                            long duration = endTime - startTime;
                            long timestamp = System.currentTimeMillis() - workloadStartTime;
    
                            for (Map.Entry<String, byte[]> entry : returnedValues.entrySet()) {
                                String key = entry.getKey();
                                byte[] value = entry.getValue();
                                if (value != null) {
                                    if (!("value" + key.substring(3)).equals(new String(value))) {
                                        Logger.log("Value mismatch for key " + key, Logger.LogLevel.ERROR);
                                    }
                                }
                            }
    
                            datapointsLock.lock();
                            try {
                                responseTimes.add(duration);
                                timestamps.add(timestamp);
                            } finally {
                                datapointsLock.unlock();
                            }
                        } catch (IOException e) {
                            Logger.log("MultiGet failed: " + e.getMessage(), Logger.LogLevel.ERROR);
                        }
                    });
    
                    multiGetThread.start();
                } catch (Exception e) {
                    Logger.log("Error during multiGet operation: " + e.getMessage(), Logger.LogLevel.ERROR);
                }
            });
        }

        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }

        generateGraph(responseTimes, timestamps, "MultiGet Operation Response time over time for a Server with " + numBuckets + " bucket(s) and " + maxClients + " client(s)", "1000 random key value pairs, 10 keys per multiget");
    }

    public void workload4() throws IOException {
        Logger.log("Running workload 4", Logger.LogLevel.INFO);

        ExecutorService executorService = Executors.newFixedThreadPool(maxClients);
        List<Long> responseTimes = new ArrayList<>();
        List<Long> timestamps = new ArrayList<>();
        ReentrantLock datapointsLock = new ReentrantLock();
        
        Logger.log("Populating database.", Logger.LogLevel.INFO);
        Thread put = new Thread(() -> {
            try {
                put("key", ("value").getBytes());
            } catch (IOException e) {
                System.out.println("Error during put operation: " + e.getMessage());
            }
        });
        put.start();
    
        for (int i = 1; i <= 100; i++) {
            final long workloadStartTime = System.currentTimeMillis();
            executorService.submit(() -> {
                Thread multiGetThread = new Thread(() -> {
                    try {
                        Set<String> keys = new HashSet<>();
                        for (int j = 1; j <= 10; j++) {
                            keys.add("key");
                        }
    
                        long startTime = System.nanoTime();
                        Map<String, byte[]> returnedValues = multiGet(keys);
                        long endTime = System.nanoTime();
                        long duration = endTime - startTime;
                        long timestamp = System.currentTimeMillis() - workloadStartTime;
    
                        for (Map.Entry<String, byte[]> entry : returnedValues.entrySet()) {
                            String key = entry.getKey();
                            byte[] value = entry.getValue();
                            if (value != null) {
                                if (!("value".equals(new String(value)))) {
                                    Logger.log("Value mismatch for key " + key, Logger.LogLevel.ERROR);
                                }
                            }
                        }
    
                        datapointsLock.lock();
                        try {
                            responseTimes.add(duration);
                            timestamps.add(timestamp);
                        } finally {
                            datapointsLock.unlock();
                        }
                    } catch (IOException e) {
                        Logger.log("MultiGet failed: " + e.getMessage(), Logger.LogLevel.ERROR);
                    }
                });
                multiGetThread.start();
            });
        }

        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }

        generateGraph(responseTimes, timestamps, "MultiGet Operation Response time over time for a Server with " + numBuckets + " bucket(s) and " + maxClients + " client(s)", "100 multiget operations on the same key (10 keys per multiget)");
    }

    public void put(String key, byte[] value) throws IOException {
        ClientLibrary client = new ClientLibrary(HOST, PORT);
        try {
            client.put(key, value);
            client.close();
        } catch (IOException e) {
            Logger.log("Put failed for key " + key + ": " + e.getMessage(), Logger.LogLevel.ERROR);
        }
    }

    public byte[] get(String key) throws IOException {
        ClientLibrary client = new ClientLibrary(HOST, PORT);
        try {
            byte[] value = client.get(key);
            client.close();
            return value;
        } catch (IOException e) {
            Logger.log("Get failed for key " + key + ": " + e.getMessage(), Logger.LogLevel.ERROR);
        }

        return null;
    }

    public Map<String, byte[]> multiGet(Set<String> keys) throws IOException {
        ClientLibrary client = new ClientLibrary(HOST, PORT);
        try {
            Map<String, byte[]> values = client.multiGet(keys);
            client.close();
            return values;
        } catch (IOException e) {
            Logger.log("MultiGet failed: " + e.getMessage(), Logger.LogLevel.ERROR);
        }

        return null;
    } 

    public void generateGraph(List<Long> responseTimes, List<Long> timestamps, String title, String subtitle) {
        XYSeries series = new XYSeries("Response Time");
    
        for (int i = 0; i < responseTimes.size(); i++) {
            series.add((Number)timestamps.get(i), (Number)(responseTimes.get(i) / 1_000_000.0));
        }
    
        XYSeriesCollection dataset = new XYSeriesCollection(series);
    
        JFreeChart chart = ChartFactory.createXYLineChart(
                title,
                "Time (ms)",
                "Response Time (ms)",
                dataset
        );

        if (subtitle != null && !subtitle.isEmpty()) {
            chart.addSubtitle(new TextTitle(subtitle));
        }
    
        ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new java.awt.Dimension(800, 600));
        javax.swing.JFrame frame = new javax.swing.JFrame();
        frame.setDefaultCloseOperation(javax.swing.JFrame.EXIT_ON_CLOSE);
        frame.getContentPane().add(chartPanel);
        frame.pack();
        frame.setVisible(true);
    }
}
