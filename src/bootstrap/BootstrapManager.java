package bootstrap;

import bootstrapUtil.NodeManager;
import common.KeyTransferService;
import common.KeyValueStore;
import bootstrapUtil.RangeManager;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class BootstrapManager {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java BootstrapManager.java <bnConfigFile>");
            return;
        }

        String configFile = args[0];
        try (BufferedReader br = new BufferedReader(new FileReader(configFile))) {
            br.readLine();                                      // Skip first line as Server ID is always 0
            int serverPort = Integer.parseInt(br.readLine());   // Second line is server port

            KeyValueStore keyValueStore = new KeyValueStore();  // Create the KeyStore (common across the server & client CLI)
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(" ");     // Split key value
                int key = Integer.parseInt(parts[0]);       // Get the key
                String value = parts[1];                    // Get the Value

                keyValueStore.insert(key, value);           // Insert the Key-Value pair
            }
            System.out.println("Inserted Initial Key-Values");

            RangeManager rangeManager = new RangeManager();
            KeyTransferService keyTransferService = new KeyTransferService(keyValueStore);
            NodeManager nodeManager = new NodeManager(serverPort);
            BootstrapServer server = new BootstrapServer(serverPort, keyValueStore,
                    rangeManager, keyTransferService, nodeManager);
            new Thread(server::start).start();              // Start server in a new thread

            BootstrapCLI clientCLI = new BootstrapCLI(keyValueStore, rangeManager, nodeManager, serverPort);
            new Thread(clientCLI::startCLI).start();        // Start the Client CLI

        } catch (IOException e) {
            System.out.println("Error reading config file: " + e.getMessage());
        } catch (NumberFormatException e) {
            System.out.println("Invalid configuration format: " + e.getMessage());
        }
    }
}
