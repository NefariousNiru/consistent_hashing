package bootstrap;

import util.KeyValueStore;
import java.io.BufferedReader;
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
                // Handle each client connection in a separate thread.
                threadPool.execute(() -> handleClient(clientSocket));
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
        if (tokens.length >= 2) {
            String command = tokens[0].toUpperCase();
            String nodeId = tokens[1];
            if ("JOIN".equals(command)) {
                System.out.println("Processing join for node " + nodeId);
                // Here, additional logic would compute and assign key ranges.
                out.println("JOIN OK");
            } else if ("EXIT".equals(command)) {
                System.out.println("Processing exit for node " + nodeId);
                // Here, additional logic would handle key transfer and range updates.
                out.println("EXIT OK");
            } else {
                out.println("UNKNOWN COMMAND");
            }
        } else {
            out.println("INVALID FORMAT");
        }
    }
}
