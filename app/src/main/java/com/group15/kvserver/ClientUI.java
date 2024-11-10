package com.group15.kvserver;

import java.io.IOException;
import java.util.*;

public class ClientUI {
    public static void main(String[] args) {
        try (Scanner scanner = new Scanner(System.in)) {
            ClientLibrary client = new ClientLibrary("localhost",12345);

            System.out.println("Choose an option: ");
            System.out.println("1. Register ");
            System.out.println("2. Log in ");
            int choice = scanner.nextInt();
            scanner.nextLine();

            System.out.println("Enter your username: ");
            String username = scanner.nextLine();
            System.out.println("Enter your password: ");
            String password = scanner.nextLine();

            boolean authenticated = false;
            if (choice == 1) {
                authenticated = client.register(username, password);
            }
            else if (choice == 2) {
                authenticated = client.authenticate(username, password);
            }

            if (authenticated) {
                System.out.println("You have successfully registered!");
                boolean running = true;
                while (running) {
                    System.out.println("Choose an operation: ");
                    System.out.println("1. Put");
                    System.out.println("2. Get");
                    System.out.println("3. MultiPut");
                    System.out.println("4. MultiGet");
                    System.out.println("5. Exit");
                    int operation = scanner.nextInt();
                    scanner.nextLine();

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
