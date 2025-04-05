package bootstrap;

import bootstrapUtil.ClientFunctions;
import bootstrapUtil.NodeManager;
import bootstrapUtil.RangeManager;
import common.KeyValueStore;
import common.NodeInfo;
import common.Range;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

public class BootstrapCLI {
    private final RangeManager rangeManager;
    private KeyValueStore keyValueStore;
    private NodeManager nodeManager;
    private final int port;

    public BootstrapCLI(KeyValueStore keyValueStore, RangeManager rangeManager,
                        NodeManager nodeManager, int port) {
        this.keyValueStore = keyValueStore;
        this.rangeManager = rangeManager;
        this.nodeManager = nodeManager;
        this.port = port;
    }

    private String forwardToSuccessor(ClientFunctions clientFunctions, String message) {
        NodeInfo successor = nodeManager.getNodeById(0).getSuccessor();
        if (successor == null) {
            return null;
        }
        try (Socket socket = new Socket(successor.getIp(), successor.getPort());
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream())))
        {
            String request = clientFunctions + " " + "0" + " " + port + " " + message;
            out.println(request);
            return in.readLine();
        } catch (IOException e) {
            return "Error forwarding lookup: " + e.getMessage();
        }
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
                    value = forwardToSuccessor(ClientFunctions.LOOKUP, Integer.toString(key)); // Forward request to successor
                    if (value == null || value.equals("null")) System.out.println("Key " + key + " not found.");
                    else System.out.println("Value for key " + key + " is: " + value);
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
                Range range = rangeManager.getRangeForNode(0);
                boolean inRange;
                if (range.getStart() <= range.getEnd()) {
                    // Non-wrapping range: key is valid if it's between start and end
                    inRange = (key >= range.getStart() && key <= range.getEnd());
                } else {
                    // Wrapping range: key is valid if it's >= start OR <= end
                    inRange = (key >= range.getStart() || key <= range.getEnd());
                }
                if (inRange) {
                    int result = keyValueStore.insert(key, value);
                    if (result == 0) {
                        System.out.println("Insertion successful for key " + key);
                    } else {
                        System.out.println("Key " + key + " already exists.");
                    }
                } else {
                    String response = forwardToSuccessor(ClientFunctions.INSERT, key + " " + value);
                    if (response == null || response.equals("null"))
                        System.out.println("Key " + key + " not inserted");
                    else
                        System.out.println(response);
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
                    String value = forwardToSuccessor(ClientFunctions.DELETE, Integer.toString(key));
                    if (value == null || value.equals("null")) System.out.println("Key " + key + " not found.");
                    else System.out.println(value);
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
                System.exit(0);
                break;
            }

            parseCommand(command);
        }

        scanner.close();
    }
}
