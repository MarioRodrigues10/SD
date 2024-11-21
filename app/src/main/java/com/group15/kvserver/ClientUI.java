package com.group15.kvserver;

import java.io.IOException;
import java.util.*;

public class ClientUI {
    public static void main(String[] args) {
        try (Scanner scanner = new Scanner(System.in)) {
            ClientLibrary client = new ClientLibrary("localhost",12345);

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

            if (choice == 3) return;

            System.out.println("Enter your username: ");
            String username = scanner.nextLine();
            System.out.println("Enter your password: ");
            String password = scanner.nextLine();

            boolean authenticated = false;
            if (choice == 1) {
                authenticated = client.register(username, password);
            }
            else {
                authenticated = client.authenticate(username, password);
            }

            if (authenticated) {
                System.out.println("You have successfully registered!");
                boolean running = true;
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
                            String key = scanner.nextLine();
                            System.out.println("Value: ");
                            byte[] value = scanner.nextLine().getBytes();
                            client.put(key, value);
                            break;

                        case 2:
                            System.out.println("Key: ");
                            key = scanner.nextLine();
                            value = client.get(key);
                            System.out.println(value != null ? new String(value) : "Chave não encontrada.");
                            break;

                        case 3:
                            System.out.println("How many key-value do you want to put?");
                            int n = scanner.nextInt();
                            scanner.nextLine();
                            Map<String, byte[]> map = new HashMap<>();
                            for (int i = 0; i < n; i++) {
                                System.out.println("Key: ");
                                key = scanner.nextLine();
                                System.out.println("Value: ");
                                value = scanner.nextLine().getBytes();
                                map.put(key, value);
                            }
                            client.multiPut(map);
                            break;

                        case 4:
                            System.out.println("How many key-value do you want to get?");
                            n = scanner.nextInt();
                            scanner.nextLine();
                            Set<String> keys = new HashSet<>();
                            for (int i = 0; i < n; i++) {
                                System.out.println("Key: ");
                                key = scanner.nextLine();
                                keys.add(key);
                            }
                            Map<String,byte[]> result = client.multiGet(keys);
                            for (Map.Entry<String, byte[]> entry : result.entrySet()) {
                                System.out.println(entry.getKey() + " : " + new String(entry.getValue()));
                            }
                            break;

                        case 5:
                            System.out.println("Key: ");
                            key = scanner.nextLine();
                            System.out.println("Condition Key: ");
                            String keyCond = scanner.nextLine();
                            System.out.println("Condition Value: ");
                            byte[] valueCond = scanner.nextLine().getBytes();
                            byte[] resultValue = client.getWhen(key, keyCond, valueCond);
                            System.out.println(resultValue != null ? new String(resultValue) : "Key not found.");
                            break;

                        case 6:
                            running = false;
                            break;

                        default:
                            System.out.println("Invalid operation!");
                            break;
                    }
                }
            } else {
                System.out.println("Authentication failed!");
            }
            client.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
