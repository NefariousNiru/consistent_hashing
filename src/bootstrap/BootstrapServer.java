package bootstrap;

import common.*;
import bootstrapUtil.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static common.NameServerFunctions.EXIT;

public class BootstrapServer {
    private int port;
    private boolean running;
    private KeyValueStore keyValueStore;
    private ExecutorService threadPool;
    private RangeManager rangeManager;
    private KeyTransferService keyTransferService;
    private NodeManager nodeManger;

    public BootstrapServer(int port, KeyValueStore keyValueStore,
                           RangeManager rangeManager, KeyTransferService keyTransferService,
                           NodeManager nodeManager) {
        this.port = port;
        this.keyValueStore = keyValueStore;
        this.threadPool = Executors.newCachedThreadPool();
        this.rangeManager = rangeManager;
        this.keyTransferService = keyTransferService;
        this.nodeManger = nodeManager;
    }

    public void start() {
        running = true;
        try(ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Bootstrap Server started on port " + port);
            while (running) {
                Socket clientSocket = serverSocket.accept();
                threadPool.execute(() -> handleClient(clientSocket));   // Handle each client connection in a thread pool.
            }
        } catch (Exception e) {
            System.out.println("Error starting server: " + e.getMessage());
        } finally {
            threadPool.shutdown();
        }
    }

    private void handleClient(Socket clientSocket) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true))
        {
            String message = in.readLine();
            if (message != null)
                parseNodeMessage(clientSocket, out, in, message);
        }
        catch (Exception e) {
            System.out.println("Error handling client: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (Exception ex) {
                System.out.println("Encountered a problem while closing socket");
            }
        }
    }

    private void parseNodeMessage(Socket clientSocket, PrintWriter out, BufferedReader in, String message) {
        System.out.println("Received message: " + message);
        String[] tokens = message.split("\\s+");
        if (tokens.length >= 3) {

            Response response;
            String command = tokens[0].toUpperCase();
            int clientNodeId = Integer.parseInt(tokens[1]);
            int clientPort = Integer.parseInt(tokens[2]);
            String clientIP = clientSocket.getInetAddress().getHostAddress();
            NodeInfo requestNode = new NodeInfo(clientNodeId, clientIP, clientPort);
            NodeInfo predNode;
            NodeInfo succNode;
            try {
                switch(NameServerFunctions.valueOf(command)) {
                    case ENTER:
                        System.out.println("Processing entry of Node " + clientNodeId);

                        if ((response = nodeManger.addNode(requestNode)).getCode() == -1) {
                            out.println(response.getMessage());
                            break;
                        }

                        if ((response = rangeManager.addNode(clientNodeId)).getCode() == -1){
                            nodeManger.removeNode(requestNode);
                            out.println(response.getMessage());
                            break;
                        }

                        predNode = requestNode.getPredecessor();
                        succNode = requestNode.getSuccessor();

                        out.println("ENTER OK" + " Predecessor: " + predNode.toString() + " Successor: " + succNode.toString());
                        break;
                    case EXIT:
                        System.out.println("Processing exit for node " + clientNodeId);

                        if ((response = nodeManger.removeNode(requestNode)).getCode() == -1) {
                            out.println(response.getMessage());
                            break;
                        }

                        if ((response = rangeManager.removeNode(clientNodeId)).getCode() == -1){
                            nodeManger.addNode(requestNode);
                            out.println(response.getMessage());
                            break;
                        }
                        out.println(EXIT + " OK");
                        break;
                    case SEND_KEYS:
                        System.out.println("Processing SEND_KEYS for node " + clientNodeId);
                        Range range = sendKeys(out, clientNodeId);

                        message = in.readLine();
                        if (message.equals("RECEIVED_OK")){
                            deleteKeys(range);
                        }
                        break;
                    case RECEIVE_KEYS:
                        System.out.println("Receiving keys from predecessor node");
                        if (tokens.length < 6) {
                            System.out.println("Invalid Request");
                            break;
                        }
                        out.println("SEND_OK");

                        int predId = Integer.parseInt(tokens[3]);
                        String predIp = tokens[4];
                        int predPort = Integer.parseInt(tokens[5]);

                        receiveKeys(in);
                        out.println("RECEIVED_OK");

                        NodeInfo bootstrapNode = nodeManger.getNodeById(0);
                        bootstrapNode.setPredecessor(new NodeInfo(predId, predIp, predPort));
                        break;
                    case UPDATE_SUCCESSOR:
                        break;
                    default: break;
                }
            } catch (IllegalArgumentException e) {
                out.println("INVALID COMMAND");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            out.println("Usage command <id> <nodeId> <localPort>. Commands supported ENTER, EXIT, SEND_KEYS");
        }
    }

    private Range sendKeys(PrintWriter out, int nodeId) {
        Range range = rangeManager.getRangeForNode(nodeId);
        keyTransferService.sendKeyValueRange(out, range);
        return range;
    }

    private void deleteKeys(Range range) {
        for (int key = range.getStart(); key <= range.getEnd(); key ++)
            keyValueStore.delete(key);
    }

    private void receiveKeys(BufferedReader in) {
        try {
            String received = in.readLine();
            String[] lines = received.split("%0A");
            for (String line : lines) {
                if (line.trim().isEmpty()) continue;
                if (line.equals("FIN"))
                    break;  // Receive key-value pairs until we encounter a termination marker ("FIN").

                String[] parts = line.split(":");
                if (parts.length < 2) {
                    System.out.println("Invalid key-value pair: " + line);
                    continue;
                }

                try {
                    int key = Integer.parseInt(parts[0].trim());
                    String value = parts[1].trim();
                    keyValueStore.insert(key, value);       // Insert the key-value pair into the local store
                    System.out.println("Received key " + key + " with value " + value);
                } catch (NumberFormatException e) {
                    System.out.println("Invalid key format in line: " + line);
                }
            }
        } catch (IOException e) {
            System.out.println("Error receiving keys due to I/O Exception: " + e.getMessage());
        }
    }
}

