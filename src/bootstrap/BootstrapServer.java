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
            int nodeId = Integer.parseInt(tokens[1]);
            int nsPort = Integer.parseInt(tokens[2]);
            String clientIP = clientSocket.getInetAddress().getHostAddress();

            try {
                switch(NameServerFunctions.valueOf(command)) {
                    case ENTER:
                        System.out.println("Processing entry of Node " + nodeId);
                        NodeInfo newNode = new NodeInfo(nodeId, clientIP, nsPort);

                        if ((response = nodeManger.addNode(newNode)).getCode() == -1) {
                            out.println(response.getMessage());
                            break;
                        }

                        if ((response = rangeManager.addNode(nodeId)).getCode() == -1){
                            nodeManger.removeNode(newNode);
                            out.println(response.getMessage());
                            break;
                        }

                        NodeInfo predNode = newNode.getPredecessor();
                        NodeInfo succNode = newNode.getSuccessor();

                        out.println("ENTER OK" + " Predecessor: " + predNode.toString() + " Successor: " + succNode.toString());
                        break;
                    case EXIT:
                        // Additional logic for EXIT (e.g., handle key transfer and range updates)
                        // Unimplemented rangeManager and transfer keys service
                        System.out.println("Processing exit for node " + nodeId);
                        if ((response = rangeManager.removeNode(nodeId)).getCode() == -1){
                            out.println(response.getMessage());
                            break;
                        }
                        receiveKeys(out, in, nodeId);
                        break;
                    case SEND_KEYS:
                        System.out.println("Processing SEND_KEYS for node " + nodeId);
                        Range range = sendKeys(out, nodeId);

                        message = in.readLine();
                        if (message.equals("RECEIVED_OK")){
                            deleteKeys(range);
                        }
                        break;
                    case RECEIVE_KEYS:
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

    private void receiveKeys(PrintWriter out, BufferedReader in, int nodeId) throws IOException {
        out.println("READY_TO_RECEIVE");
        String keyVals;
        while ((keyVals = in.readLine()) != null) {
            if (keyVals.equals("FIN")) {
                out.println("EXIT OK");
                break;
            }
            System.out.println(keyVals); // logic to move keys implemented in key transfer service
        }
    }
}

