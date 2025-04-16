package nameservers;

import common.KeyTransferService;
import common.KeyValueStore;
import common.NameServerFunctions;
import common.NodeInfo;
import nameservers.namserverUtil.ResponseParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

import static common.NameServerFunctions.ENTER;
import static common.NameServerFunctions.EXIT;

public class NameServerCLI {
    private NameServer nameServer;
    private String bootstrapIP;
    private int bootstrapPort;
    private int id;
    private int port;
    private KeyValueStore keyValueStore;
    private KeyTransferService keyTransferService;

    public NameServerCLI(int id, int port, String bootstrapIP,
                         int bootstrapPort, KeyValueStore keyValueStore,
                         KeyTransferService keyTransferService, NameServer nameServer) {
        this.id = id;
        this.port = port;
        this.bootstrapIP = bootstrapIP;
        this.bootstrapPort = bootstrapPort;
        this.keyValueStore = keyValueStore;
        this.keyTransferService = keyTransferService;
        this.nameServer = nameServer;
    }

    public boolean enterNetwork() {
        if (nameServer.isJoined()) {
            System.out.println("Already joined the network.");
            return false;
        }
        try (Socket socket = new Socket(bootstrapIP, bootstrapPort)) {
            socket.setSoTimeout(5000); // Set a timeout of 5 seconds for reading response
            try (PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream())))
            {
                String joinMessage = ENTER + " " + id + " " + port;
                out.println(joinMessage);
                System.out.println("Sent join request: " + joinMessage);

                String response = in.readLine();        // Receive successor and predecessor
                System.out.println("Bootstrap response: " + response);

                NodeInfo[] neighbors = ResponseParser.parseBootstrapResponse(response);
                for (int i = 0; i <= 1 ; i++) {
                    // if neighbor is bootstrap node
                    if (neighbors[i].getIp().equals("N/A")) {
                        neighbors[i].setIp(bootstrapIP);
                        neighbors[i].setPort(bootstrapPort);
                    }
                }

                NodeInfo nodeInfo = nameServer.getNodeInfo();
                nodeInfo.setPredecessor(neighbors[0]);
                nodeInfo.setSuccessor(neighbors[1]);
                nameServer.markAsJoined();
            }
        } catch (java.net.SocketTimeoutException ste) {
            System.out.println("Timed out waiting for bootstrap response.");
            return false;
        } catch (Exception e) {
            System.out.println("Error entering network: " + e.getMessage());
            return false;
        }
        return true;
    }

    public boolean exitNetwork() {
        if (!nameServer.isJoined()) {
            System.out.println("Not currently joined the network. No keys to transfer");
            return false;
        }
        try(Socket socket = new Socket(bootstrapIP, bootstrapPort)) {
            socket.setSoTimeout(5000);  // Set a timeout of 5 seconds for reading response
            try(PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream())))
            {
                String exitMessage = EXIT + " " + id + " " + port;
                out.println(exitMessage);
                System.out.println("Sent exit request: " + exitMessage);

                String response = in.readLine();
                System.out.println("Bootstrap response: " + response);
                return response.equals(EXIT + " OK");
            } catch(Exception e) {
                System.out.println("Error exiting network: " + e.getMessage());
                return false;
            }
        } catch (java.net.SocketTimeoutException ste) {
            System.out.println("Timed out waiting for bootstrap response.");
            return false;
        } catch (Exception e) {
            System.out.println("Error exiting network: " + e.getMessage());
            return false;
        }
    }

    public void startCLI() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("NameServer CLI started. Available commands: enter, exit");
        while (true) {
            System.out.print("NS> ");
            String input = scanner.nextLine().trim().toUpperCase();
            try {
                switch (NameServerFunctions.valueOf(input)) {
                    case ENTER:
                        if(enterNetwork()){
                            nameServer.receiveKeysOnEntry();
                            nameServer.announceEntryToPredecessor(nameServer.getNodeInfo().getPredecessor());
                        }
                        break;
                    case EXIT:
                        if (exitNetwork()){
                            nameServer.sendKeysOnExit();
                            nameServer.announceExitToPredecessor(
                                    nameServer.getNodeInfo().getPredecessor(),
                                    nameServer.getNodeInfo().getSuccessor()
                            );
                            System.out.println("Exiting NameServer CLI.");
                            System.exit(0);
                        }
                        break;
                    case PRINT:
                        keyValueStore.print_keys();
                        break;
                    case NEIGHBOR:
                        System.out.println("Predecessor :" + nameServer.getNodeInfo().getPredecessor());
                        System.out.println("Successor :" + nameServer.getNodeInfo().getSuccessor());
                    default: break;
                }
            } catch (IllegalArgumentException e) {
                System.out.println("Unknown command. Available commands: enter, exit");
            }
        }
    }
}
