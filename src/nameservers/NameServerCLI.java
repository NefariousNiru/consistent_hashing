package nameservers;

import common.KeyTransferService;
import common.KeyValueStore;
import common.NameServerFunctions;
import common.NodeInfo;
import namserverUtil.ResponseParser;

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
                System.out.println("My Node" + nodeInfo);
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

    public int exitNetwork() {
        if (!nameServer.isJoined()) {
            System.out.println("Not currently joined the network. No keys to transfer");
            return 0;
        }
        try(Socket socket = new Socket(bootstrapIP, bootstrapPort)) {
            socket.setSoTimeout(5000);  // Set a timeout of 5 seconds for reading response
            try(PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream())))
            {
                String exitMessage = EXIT + " " + id;
                out.println(exitMessage);
                System.out.println("Sent exit request: " + exitMessage);

                String response = in.readLine();
                System.out.println("Bootstrap response: " + response);

                if (!response.equals("READY_TO_RECEIVE")) {
                    System.out.println("Server cannot receive keys. Graceful shutdown aborted.");
                    return -1;
                }

                int i = 0;
                while (i < 5) {
                    // transmit keys
                    out.println("Key:Value");
                    i += 1;
                }
                out.println("FIN");

                response = in.readLine();
                System.out.println("Bootstrap response: " + response);
                if (response.equals("EXIT OK")) {
                    nameServer.markAsJoinedFalse();
                    return 0;
                }

                System.out.println("Server cannot receive keys. Graceful shutdown aborted.");
                return -1;
            } catch(Exception e) {
                System.out.println("Error exiting network: " + e.getMessage());
                return -1;
            }
        } catch (java.net.SocketTimeoutException ste) {
            System.out.println("Timed out waiting for bootstrap response.");
            return -1;
        } catch (Exception e) {
            System.out.println("Error exiting network: " + e.getMessage());
            return -1;
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
                        if(enterNetwork() && nameServer.isJoined())
                            nameServer.receiveKeys();;
                        break;
                    case EXIT:
                        if (exitNetwork() == 0){
                            System.out.println("Exiting NameServer CLI.");
                            System.exit(0);
                        }
                        break;
                    case PRINT:
                        keyValueStore.print_keys();
                        break;
                    default: break;
                }
            } catch (IllegalArgumentException e) {
                System.out.println("Unknown command. Available commands: enter, exit");
            }
        }
    }
}
