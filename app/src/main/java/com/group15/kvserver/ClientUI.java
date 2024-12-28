package com.group15.kvserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.InputMismatchException;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

public class ClientUI {
    public static void main(String[] args) {
        try (Scanner scanner = new Scanner(System.in)) {
            ClientLibrary client = new ClientLibrary("localhost", 12345);

            int choice = 0;
            boolean validChoice = false;
            while (!validChoice) {
                System.out.println("Choose an option: ");
                System.out.println("1. Register ");
                System.out.println("2. Log in ");
                System.out.println("3. Exit");

                try {
                    choice = scanner.nextInt();
                    scanner.nextLine();
                    if (choice == 1 || choice == 2 || choice == 3) {
                        validChoice = true;
                    } else {
                        System.out.println("Invalid option! Please choose 1 to Register, 2 to Log in, or 3 to Exit.");
                    }
                } catch (InputMismatchException e) {
                    System.out.println("Invalid input! Please enter a number (1, 2 or 3).");
                    scanner.nextLine();
                }
            }

            if (choice == 3) {
                client.close();
                return;
            }

            System.out.println("Enter your username: ");
            String username = scanner.nextLine();
            System.out.println("Enter your password: ");
            String password = scanner.nextLine();

            final boolean[] authenticated = new boolean[1];
            final int finalChoice = choice;
            Thread authThread = new Thread(() -> {
                if (finalChoice == 1) {
                    try {
                        authenticated[0] = client.register(username, password);
                    } catch (IOException e) {
                        System.out.println("Error during registration: " + e.getMessage());
                        authenticated[0] = false;
                    }
                } else {
                    try {
                        authenticated[0] = client.authenticate(username, password);
                    } catch (IOException e) {
                        System.out.println("Error during authentication: " + e.getMessage());
                        authenticated[0] = false;
                    }
                }
            });
            
            authThread.start();

            try {
                authThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println("Thread interrupted during authentication.");
                return;
            }

            if (authenticated[0]) {
                System.out.println("You have successfully registered!");
                boolean running = true;
                List<Thread> threads = new ArrayList<>();

                while (running) {
                    int operation = 0;
                    validChoice = false;
                    while (!validChoice) {
                        System.out.println("Choose an operation: ");
                        System.out.println("1. Put");
                        System.out.println("2. Get");
                        System.out.println("3. MultiPut");
                        System.out.println("4. MultiGet");
                        System.out.println("5. GetWhen");
                        System.out.println("6. Exit");

                        try {
                            operation = scanner.nextInt();
                            scanner.nextLine();
                            if (operation >= 1 && operation <= 6) {
                                validChoice = true;
                            } else {
                                System.out.println("Invalid operation! Please choose a valid operation.");
                            }
                        } catch (InputMismatchException e) {
                            System.out.println("Invalid input! Please enter a valid number between 1 and 5.");
                            scanner.nextLine();
                        }
                    }

                    switch (operation) {
                        case 1:
                        System.out.println("Key: ");
                        final String key = scanner.nextLine();
                        System.out.println("Value: ");
                        final byte[] value = scanner.nextLine().getBytes();
                        Thread put = new Thread(() -> {
                            try {
                                client.put(key, value);
                            } catch (IOException e) {
                                System.out.println("Error during put operation: " + e.getMessage());
                            }
                        });
                        put.start();
                        threads.add(put);
                        break;

                        case 2:
                            System.out.println("Key: ");
                            String getKey = scanner.nextLine();
                            Thread get = new Thread(() -> {
                                try {
                                    byte[] result = client.get(getKey);
                                    System.out.println("Get operation: Key = " + getKey + ", Value = " + (result != null ? new String(result) : "Key not found."));
                                } catch (IOException e) {
                                    System.out.println("Error during get operation: " + e.getMessage());
                                }
                            });
                            get.start();
                            threads.add(get);
                            break;

                        case 3:
                            System.out.println("How many key-value do you want to put?");
                            int n = scanner.nextInt();
                            scanner.nextLine();
                            Map<String, byte[]> map = new HashMap<>();
                            for (int i = 0; i < n; i++) {
                                System.out.println("Key: ");
                                final String key1 = scanner.nextLine();
                                System.out.println("Value: ");
                                final byte[] value1 = scanner.nextLine().getBytes();
                                map.put(key1, value1);
                            }
                            Thread multiPut = new Thread(() -> {
                                try {
                                    client.multiPut(map);
                                } catch (IOException e) {
                                    System.out.println("Error during multiPut operation: " + e.getMessage());
                                }
                            });
                            multiPut.start();
                            threads.add(multiPut);
                            break;

                        case 4:
                            System.out.println("How many key-value do you want to get?");
                            n = scanner.nextInt();
                            scanner.nextLine();
                            Set<String> keys = new HashSet<>();
                            for (int i = 0; i < n; i++) {
                                System.out.println("Key: ");
                                final String key2 = scanner.nextLine();
                                keys.add(key2);
                            }
                            Thread multiGet = new Thread(() -> {
                                try {
                                    Map<String, byte[]> resultMap = client.multiGet(keys);
                                    for (Map.Entry<String, byte[]> entry : resultMap.entrySet()) {
                                        System.out.println("MultiGet operation: Key = " + entry.getKey() + ", Value = " + new String(entry.getValue()));
                                    }
                                } catch (IOException e) {
                                    System.out.println("Error during multiGet operation: " + e.getMessage());
                                }
                            });
                            multiGet.start();
                            threads.add(multiGet);
                            break;

                        case 5:
                            System.out.println("Key: ");
                            final String key3 = scanner.nextLine();
                            System.out.println("Condition Key: ");
                            String keyCond = scanner.nextLine();
                            System.out.println("Condition Value: ");
                            byte[] valueCond = scanner.nextLine().getBytes();
                            Thread condition = new Thread(() -> {
                                try {
                                    byte[] resultValue = client.getWhen(key3, keyCond, valueCond);
                                    System.out.println("GetWhen operation: Key = " + key3 + ", Condition Key = " + keyCond + ", Result Value = " + (resultValue != null ? new String(resultValue) : "Key not found."));
                                } catch (IOException e) {
                                    System.out.println("Error during getWhen operation: " + e.getMessage());
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            });
                            condition.start();
                            threads.add(condition);
                            break;

                        case 6:
                            System.out.println("Disconnecting...");
                            try{
                                client.sendDisconnectMessage();
                            }
                            catch (IOException e){
                                System.out.println("Error during disconnect: " + e.getMessage());
                            }
                            running = false;
                            break;

                        default:
                            System.out.println("Invalid operation!");
                            break;
                    }
                }
                System.out.println("Closing threads...");
                for (Thread t : threads) {
                    try {
                        t.join();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        System.out.println("Thread interrupted.");
                    }
                }
                System.out.println("Bye.");

            } else {
                System.out.println("Authentication failed!");
                client.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
