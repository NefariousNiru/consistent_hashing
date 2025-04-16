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
                System.out.println("Received connection from " + clientSocket.getInetAddress().getHostAddress());
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
                // NameServerFunctionsEnum + " " + id + " " + port;
                String[] tokens = message.split("\\s+");
                String command = tokens[0].toUpperCase();
                int clientNodeId = Integer.parseInt(tokens[1]);
                int clientPort = Integer.parseInt(tokens[2]);
                String clientIp = clientSocket.getInetAddress().getHostAddress();

                switch (NameServerFunctions.valueOf(command)) {
                    case SEND_KEYS:
                        System.out.println("Processing SEND_KEYS for node " + clientNodeId);
                        Range range = sendKeys(out, clientNodeId);

                        message = in.readLine();
                        if (message.equals("RECEIVED_OK")){
                            deleteKeys(range);
                        }

                        nodeInfo.setPredecessor(new NodeInfo(clientNodeId, clientIp, clientPort));    // set new predecessor
                        break;
                    case RECEIVE_KEYS:
                        System.out.println("Receiving keys from predecessor node");
                        if (tokens.length < 6) {
                            System.out.println("Invalid Request");
                            break;
                        }
                        out.println("SEND_OK");

                        int clientPredecessorId = Integer.parseInt(tokens[3]);
                        String clientPredecessorIp = tokens[4];
                        int clientPredecessorPort = Integer.parseInt(tokens[5]);

                        receiveKeys(in);
                        out.println("RECEIVED_OK");

                        nodeInfo.setPredecessor(
                                new NodeInfo(clientPredecessorId, clientPredecessorIp, clientPredecessorPort)
                        ); // set new predecessor
                        break;
                    case UPDATE_SUCCESSOR:
                        if (tokens.length == 3)
                            // When node enters and sends a update successor request
                            nodeInfo.setSuccessor(new NodeInfo(clientNodeId, clientIp, clientPort));      // set new successor
                        else {
                            // When a node exists it sends its successors IP which becomes 'this' successor IP
                            String successorIp = tokens[3];
                            nodeInfo.setSuccessor(new NodeInfo(clientNodeId, successorIp, clientPort));
                        }
                        break;
                    case LOOKUP:
                        int key = Integer.parseInt(tokens[3]);
                        System.out.println("Received Lookup request for key: " + key);
                        String value  = keyValueStore.lookup(key);      // check if this name server has it
                        if (value != null) {
                            System.out.println("Key " + key + " found");
                            out.println(value);                         // If found return key
                            break;
                        }
                        System.out.println("Key " + key + " not found -> forwarding");
                        value = forwardToSuccessor(LOOKUP, Integer.toString(key));
                        out.println(value);                             // return any value successors found
                        break;
                    case INSERT:
                        key = Integer.parseInt(tokens[3]);
                        value = tokens[4];
                        System.out.println("Received Insert Request for Key: " + key + " with Value: " + value);
                        range = getSendRange(id);
                        if (range.getStart() <= key && key <= range.getEnd()) {
                            int result = keyValueStore.insert(key, value);
                            if(result == 0) {
                                out.println("Insertion successful for key " + key);
                            } else {
                                out.println("Key " + key + " already exists.");
                            }
                        }
                        else {
                            System.out.println("Key " + key + " not found -> forwarding");
                            String response = forwardToSuccessor(NameServerFunctions.INSERT, key + " " + value);
                            out.println(response);
                        }
                        break;
                    case DELETE:
                        key = Integer.parseInt(tokens[3]);
                        System.out.println("Received delete request for key: " + key);
                        int result = keyValueStore.delete(key);      // check if this name server has it
                        if (result == 0) {
                            System.out.println("Key " + key + " found");
                            out.println("Key " + key + " deleted");                         // If found return key
                            break;
                        }
                        System.out.println("Key " + key + " not found -> forwarding");
                        value = forwardToSuccessor(LOOKUP, Integer.toString(key));
                        out.println(value);                             // return any value successors found
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
     * Initiated by this node. Send a RECEIVE_KEYS request.
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
            if (predId == 0) predIp = bootstrapIP;
            else predIp = nodeInfo.getPredecessor().getIp();
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

            sendKeys(out, id);

            message = in.readLine();
            if (message.equals("RECEIVED_OK"))
                System.out.println("Successfully sent all keys");
            else System.out.println("Failed to send keys");
        } catch(Exception e) {
            System.out.println("Error during sending key: " + e.getMessage());
        }
    }

    /**
     * Function triggered when 'this node' enters the network and wants to receive keys from successor.
     * Sends a SEND_KEYS request.
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
     * Helper method to send keys to a 'PrintWrite out' source for 'this' nameservers
     * Send all the keys possessed by this server (start -> nodeId)
     */
    private Range sendKeys(PrintWriter out, int endKey) {
        Range range = getSendRange(endKey);
        System.out.println("Sending keys in range: " + range.getStart() + " to " + endKey);
        keyTransferService.sendKeyValueRange(out, range);
        return range;
    }

    /**
     * Helper method to receive keys using a 'BufferedReader in' source and store in this node
     * Receives all keys when a predecessor leaves and hands over keys or
     * When 'this node' enters the network and receives successor's keyspace (start -> this.nodeId)
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

    /**
     * Ask predecessor to set 'this node' as successor
     * @param predecessorNode: The predecessor Node of class NodeInfo
     */
    public void announceEntryToPredecessor(NodeInfo predecessorNode) {
        try (Socket socket = new Socket(predecessorNode.getIp(), predecessorNode.getPort());
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true))
        {
            System.out.println("Announcing entry to predecessor node");
            String request = UPDATE_SUCCESSOR + " " + id + " " + port;
            out.println(request);
            System.out.println("Announce success");
        } catch(Exception e) {
            System.out.println("Error during announcing entry to predecessor key: " + e.getMessage());
        }
    }

    public void announceExitToPredecessor(NodeInfo predecessorNode, NodeInfo successorNode) {
        try (Socket socket = new Socket(predecessorNode.getIp(), predecessorNode.getPort());
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true))
        {
            System.out.println("Announcing exit to predecessor node");
            String request = UPDATE_SUCCESSOR + " " + successorNode.getId() + " " + successorNode.getPort() + " " + successorNode.getIp() ;
            out.println(request);
            System.out.println("Announce success");
        } catch(Exception e) {
            System.out.println("Error during announcing entry to predecessor key: " + e.getMessage());
        }
    }

    private String forwardToSuccessor(NameServerFunctions nsf, String message) {
        NodeInfo successor = nodeInfo.getSuccessor();
        if (successor == null || successor.getId() == 0) {
            System.out.println("Forwarding aborted: No successor present or successor is Bootstrap Node");
            return null;
        }
        try (Socket socket = new Socket(successor.getIp(), successor.getPort());
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream())))
        {
            String request = nsf + " " + id + " " + port + " " + message;
            out.println(request);
            return in.readLine();
        } catch (IOException e) {
            return "Error forwarding lookup: " + e.getMessage();
        }
    }

    public NodeInfo getNodeInfo() {
        return nodeInfo;
    }

    public void markAsJoined() {
        isJoined = true;
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
