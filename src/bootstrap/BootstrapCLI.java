package bootstrap;

import bootstrapUtil.ClientFunctions;
import bootstrapUtil.NodeManager;
import bootstrapUtil.RangeManager;
import common.KeyValueStore;
import java.util.Scanner;

public class BootstrapCLI {
    private final RangeManager rangeManager;
    private KeyValueStore keyValueStore;
    private NodeManager nodeManager;

    public BootstrapCLI(KeyValueStore keyValueStore, RangeManager rangeManager, NodeManager nodeManager) {
        this.keyValueStore = keyValueStore;
        this.rangeManager = rangeManager;
        this.nodeManager = nodeManager;
    }

    public void lookupKey(String[] tokens) {
        if (tokens.length < 2) {
            System.out.println("Usage: lookup <key>");
        } else {
            try {
                int key = Integer.parseInt(tokens[1]);
                String value = keyValueStore.lookup(key);
                if(value != null) {
                    System.out.println("Value for key " + key + " is: " + value);
                } else {
                    System.out.println("Key " + key + " not found.");
                }
            } catch (NumberFormatException e) {
                System.out.println("Invalid key format. Key must be an integer.");
            }
        }
    }

    public void insertKey(String[] tokens) {
        if (tokens.length < 3) {
            System.out.println("Usage: insert <key> <value>");
        } else {
            try {
                int key = Integer.parseInt(tokens[1]);
                String value = tokens[2];
                int result = keyValueStore.insert(key, value);
                if(result == 0) {
                    System.out.println("Insertion successful for key " + key);
                } else {
                    System.out.println("Key " + key + " already exists.");
                }
            } catch (NumberFormatException e) {
                System.out.println("Invalid key format. Key must be an integer.");
            }
        }
    }

    public void deleteKey(String[] tokens) {
        if (tokens.length < 2) {
            System.out.println("Usage: delete <key>");
        } else {
            try {
                int key = Integer.parseInt(tokens[1]);
                int result = keyValueStore.delete(key);
                if(result == 0) {
                    System.out.println("Key " + key + " deleted successfully.");
                } else {
                    System.out.println("Key " + key + " not found.");
                }
            } catch (NumberFormatException e) {
                System.out.println("Invalid key format. Key must be an integer.");
            }
        }
    }

    public void parseCommand(String input) {
        String[] tokens = input.split("\\s+");
        if (tokens.length == 0) {
            return;
        }

        String command = tokens[0].toUpperCase();
        try {
            switch (ClientFunctions.valueOf(command)) {
                case LOOKUP:
                    lookupKey(tokens);
                    break;
                case INSERT:
                    insertKey(tokens);
                    break;
                case DELETE:
                    deleteKey(tokens);
                    break;
                case PRINT:
                    keyValueStore.print_keys();
                    break;
                case RANGE:
                    rangeManager.printRanges();
                    break;
                case NODES:
                    nodeManager.printNodes();
                    break;
                default: break;
            }
        } catch (IllegalArgumentException e) {
            System.out.println("Unknown command. Available commands: insert, lookup, delete, exit.");
        }
    }

    public void startCLI() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Bootstrap Node CLI started. Enter commands lookup, insert, delete (type 'exit' to quit):");

        while(true) {
            System.out.print("> ");
            String command = scanner.nextLine().trim();

            if(command.equalsIgnoreCase("exit")) {
                System.out.println("Exiting CLI...");
                break;
            }

            parseCommand(command);
        }

        scanner.close();
    }
}
