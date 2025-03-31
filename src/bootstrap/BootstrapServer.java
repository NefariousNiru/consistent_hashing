package bootstrap;

import common.KeyTransferService;
import util.KeyValueStore;
import common.NameServerFunctions;
import util.RangeManager;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BootstrapServer {
    private int port;
    private boolean running;
    private KeyValueStore keyValueStore;
    private ExecutorService threadPool;

    public BootstrapServer(int port, KeyValueStore keyValueStore) {
        this.port = port;
        this.keyValueStore = keyValueStore;
        this.threadPool = Executors.newCachedThreadPool();
    }

    public void start() {
        running = true;
        try(ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Bootstrap Server started on port " + port);
            while (running) {
                Socket clientSocket = serverSocket.accept();
                threadPool.execute(() -> handleClient(clientSocket));   // Handle each client connection in a separate thread.
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
                parseNodeMessage(out, message);
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

    private void parseNodeMessage(PrintWriter out, String message) {
        System.out.println("Received message: " + message);
        String[] tokens = message.split("\\s+");
        System.out.println(Arrays.toString(tokens));
        if (tokens.length >= 2) {
            String command = tokens[0].toUpperCase();
            int nodeId = Integer.parseInt(tokens[1]);
            try {
                switch(NameServerFunctions.valueOf(command)) {
                    case ENTER:
                        System.out.println("Processing entry for node " + nodeId);
                        // Additional logic for ENTER (e.g., assign key ranges)
                        out.println("ENTER OK");
                        break;
                    case EXIT:
                        System.out.println("Processing exit for node " + nodeId);
                        // Additional logic for EXIT (e.g., handle key transfer and range updates)
                        out.println("EXIT OK");
                        break;
                }
            } catch (IllegalArgumentException e) {
                out.println("INVALID COMMAND");
            }
        } else {
            out.println("Usage command <id>. Commands supported ENTER, EXIT");
        }
    }
}
