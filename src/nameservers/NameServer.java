package nameservers;

import common.Range;
import common.KeyTransferService;
import common.KeyValueStore;
import common.NameServerFunctions;
import common.NodeInfo;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static common.NameServerFunctions.*;

public class NameServer {
    private int id;
    private int port;
    private String bootstrapIP;
    private int bootstrapPort;
    private boolean isJoined = false;
    private KeyTransferService keyTransferService;
    private KeyValueStore keyValueStore;
    private NodeInfo nodeInfo;

    public NameServer(int id, int port, String bootstrapIP, int bootstrapPort,
                      KeyValueStore keyValueStore, KeyTransferService keyTransferService) {
        this.id = id;
        this.port = port;
        this.bootstrapIP = bootstrapIP;
        this.bootstrapPort = bootstrapPort;
        this.keyValueStore = keyValueStore;
        this.keyTransferService = keyTransferService;
        nodeInfo = new NodeInfo(id, "self", port);
    }

    public void startServer() {
        ExecutorService threadPool = Executors.newCachedThreadPool();
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("NameServer listening on port " + port);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Received connection from " + clientSocket.getInetAddress());
                threadPool.execute(() -> handleIncomingRequest(clientSocket));  // Spawn a thread in a pool
            }
        } catch (IOException e) {
            System.out.println("Error in NameServer server: " + e.getMessage());
            System.exit(-1);
        }
    }

    private void handleIncomingRequest(Socket clientSocket) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {
            String message = in.readLine();
            if (message != null) {
                String[] tokens = message.split("\\s+");  // SEND_KEYS + " " + id + " " + port;
                String command = tokens[0].toUpperCase();
                int nodeId = Integer.parseInt(tokens[1]);
                int port = Integer.parseInt(tokens[2]);
                String ip = clientSocket.getInetAddress().toString();

                switch (NameServerFunctions.valueOf(command)) {
                    case SEND_KEYS:
                        System.out.println("Processing SEND_KEYS for node " + nodeId);
                        Range range = sendKeys(out, nodeId);

                        message = in.readLine();
                        if (message.equals("RECEIVED_OK")){
                            deleteKeys(range);
                        }

                        nodeInfo.setPredecessor(new NodeInfo(nodeId, ip, port));    // set new predecessor
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
                        nodeInfo.setPredecessor(new NodeInfo(predId, predIp, predPort));
                        break;
                    case UPDATE_NEIGHBORS:
                        break;
                    default: break;
                }
            }
        } catch (IllegalArgumentException e) {
            System.out.println("Invalid Request from incoming request " + e.getMessage());
        } catch (IOException e) {
            System.out.println("Error handling incoming request: " + e.getMessage());
        } finally {
            try { clientSocket.close(); } catch (Exception ignored) { }
        }
    }

    /**
     * Send keys to successor when this node leaves.
     * Initiated by this node.
     * Successor receives keys using receiveKeys()
     */
    public void sendKeysOnExit() {
        NodeInfo successor = nodeInfo.getSuccessor();
        if (successor == null) {
            System.out.println("No successor available for sending key.");
            return;
        }

        System.out.println("Initiating key sending to successor: " + successor);
        try (Socket socket = new Socket(successor.getIp(), successor.getPort());
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream())))
        {
            int predId = nodeInfo.getPredecessor().getId();
            String predIp;
            if (predId == 0)
                predIp = bootstrapIP;
            predIp = nodeInfo.getPredecessor().getIp();
            int predPort = nodeInfo.getPredecessor().getPort();

            String transferRequest = RECEIVE_KEYS + " " + id + " " + port + " " +
                    predId + " " + predIp + " " + predPort;
            out.println(transferRequest);
            System.out.println("Sent key sending request: " + transferRequest);

            String message = in.readLine();
            if (!message.equals("SEND_OK")) {
                System.out.println("Server not ready to receive keys");
                return;
            }
            System.out.println("Sending keys...");

            sendKeys(out, id);                      // Send all keys up to this nameserver id

            message = in.readLine();
            if (message.equals("RECEIVED_OK"))
                System.out.println("Successfully sent all keys");
            else System.out.println("Failed to send keys");
        } catch(Exception e) {
            System.out.println("Error during sending key: " + e.getMessage());
        }
    }

    /**
     * Receive keys from a successor on entry of this node.
     * Initiated by this node.
     * Successor replies using the method: sendKeys()
     */
    public void receiveKeysOnEntry() {
        NodeInfo successor = nodeInfo.getSuccessor();
        if (successor == null) {
            System.out.println("No successor available for key receiving.");
            return;
        }

        System.out.println("Initiating key retrieval from successor: " + successor);
        try (Socket socket = new Socket(successor.getIp(), successor.getPort());
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream())))
        {
            String transferRequest = SEND_KEYS + " " + id + " " + port;
            out.println(transferRequest);
            System.out.println("Sent key retrieval request: " + transferRequest);

            receiveKeys(in);
            out.println("RECEIVED_OK");
        } catch(Exception e) {
            System.out.println("Error during key reception: " + e.getMessage());
        }
    }

    /**
     * Send keys to predecessor when predecessor joins and asks for transfer.
     * Initiated by predecessor node. (Response)
     * Predecessor node initiates request with the method: receiveKeysOnEntry()
     */
    private Range sendKeys(PrintWriter out, int endKey) {
        Range range = getSendRange(endKey);
        System.out.println("Sending keys in range: " + range.getStart() + " to " + endKey);
        keyTransferService.sendKeyValueRange(out, range);
        return range;
    }

    /**
     * Receive keys in this node when a predecessor leaves
     * Initiated by leaving node. (Predecessor) using method sendKeysOnExit()
     */
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

    private void deleteKeys(Range range) {
        for (int key = range.getStart(); key <= range.getEnd(); key ++)
            keyValueStore.delete(key);
    }

    public NodeInfo getNodeInfo() {
        return nodeInfo;
    }

    public void markAsJoined() {
        isJoined = true;
    }

    public void markAsJoinedFalse() {
        isJoined = false;
    }

    public boolean isJoined() {
        return isJoined;
    }

    private Range getSendRange(int endKey) {
        NodeInfo predecessor = nodeInfo.getPredecessor();
        if (predecessor != null) {
            return new Range(predecessor.getId() + 1, endKey);
        }
        return new Range(0, endKey);
    }
}
