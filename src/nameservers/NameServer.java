package nameservers;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

import static common.NameServerFunctions.ENTER;
import static common.NameServerFunctions.EXIT;

public class NameServer {
    private int id;
    private int port;
    private String bootstrapIP;
    private int bootstrapPort;
    private boolean isJoined = false;

    public NameServer(int id, int port, String bootstrapIP, int bootstrapPort) {
        this.id = id;
        this.port = port;
        this.bootstrapIP = bootstrapIP;
        this.bootstrapPort = bootstrapPort;
    }

    public void enterNetwork() {
        if (isJoined) {
            System.out.println("Already joined the network.");
            return;
        }
        try (Socket socket = new Socket(bootstrapIP, bootstrapPort)) {
            socket.setSoTimeout(5000); // Set a timeout of 5 seconds for reading response
            try (PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream())))
            {
                String joinMessage = ENTER + " " + id;
                out.println(joinMessage);
                System.out.println("Sent join request: " + joinMessage);

                String response = in.readLine();
                if (response != null) {
                    System.out.println("Bootstrap response: " + response);
                    isJoined = true;
                } else {
                    System.out.println("No response from bootstrap node.");
                }
            }
        } catch (java.net.SocketTimeoutException ste) {
            System.out.println("Timed out waiting for bootstrap response.");
        } catch (Exception e) {
            System.out.println("Error joining network: " + e.getMessage());
        }
    }

    public void exitNetwork() {
        if (!isJoined) {
            System.out.println("Not currently joined the network.");
            return;
        }
        try(Socket socket = new Socket(bootstrapIP, bootstrapPort);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream())))
            {
                String exitMessage = EXIT + " " + id;
                out.println(exitMessage);
                System.out.println("Sent exit request: " + exitMessage);

                String response = in.readLine();
                System.out.println("Bootstrap response: " + response);
                // Assume a positive response means the node has exited successfully.
                isJoined = false;
            } catch(Exception e) {
                System.out.println("Error exiting network: " + e.getMessage());
            }
    }

    public void startCLI() {
        Thread cliThread = new Thread(() ->{
            Scanner scanner = new Scanner(System.in);
            System.out.println("NameServer CLI started. Available commands: enter, exit");
            while (true) {
                System.out.print("NS> ");
                String input = scanner.nextLine().trim();
                if (input.equalsIgnoreCase("enter")) {
                    enterNetwork();
                } else if (input.equalsIgnoreCase("exit")) {
                    exitNetwork();
                    System.out.println("Exiting NameServer CLI.");
                    System.exit(0);
                    break;
                } else {
                    System.out.println("Unknown command. Available commands: enter, exit");
                }
            }
            scanner.close();
        });
        cliThread.start();
    }

    public void startServer() {
        Thread serverThread = new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                System.out.println("NameServer listening on port " + port);
                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    System.out.println("Received connection from " + clientSocket.getInetAddress());
                    // For now, simply close the connection after accepting.
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.out.println("Error in NameServer server: " + e.getMessage());
            }
        });
        serverThread.start();
    }

    private static void runNameServer(String idLine, String portLine, String bootstrapLine) {
        int id = Integer.parseInt(idLine.trim());
        int port = Integer.parseInt(portLine.trim());
        String[] bootstrapInfo = bootstrapLine.trim().split("\\s+");
        String bootstrapIP = bootstrapInfo[0];
        int bootstrapPort = Integer.parseInt(bootstrapInfo[1]);

        // Create the NameServer instance with configuration values.
        NameServer ns = new NameServer(id, port, bootstrapIP, bootstrapPort);

        // Start the server to accept incoming connections.
        ns.startServer();

        // Start the CLI for node-specific operations.
        ns.startCLI();
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java NameServer.java <nsConfigFile>");
            return;
        }

        String configFile = args[0];
        try (BufferedReader br = new BufferedReader(new FileReader(configFile))) {
            // Read configuration parameters from the file.
            String idLine = br.readLine();
            String portLine = br.readLine();
            String bootstrapLine = br.readLine();

            runNameServer(idLine, portLine, bootstrapLine);
        } catch (IOException e) {
            System.out.println("Error reading config file: " + e.getMessage());
        } catch (NumberFormatException e) {
            System.out.println("Invalid configuration format: " + e.getMessage());
        }
    }
}
