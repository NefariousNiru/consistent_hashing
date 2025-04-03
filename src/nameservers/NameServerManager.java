package nameservers;

import common.KeyTransferService;
import common.KeyValueStore;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class NameServerManager {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java NameServerManager.java <nsConfigFile>");
            return;
        }

        String configFile = args[0];
        try (BufferedReader br = new BufferedReader(new FileReader(configFile))) {
            // Read configuration parameters from the file.
            String idLine = br.readLine();
            String portLine = br.readLine();
            String bootstrapLine = br.readLine();

            int nodeId = Integer.parseInt(idLine.trim());
            int localPort = Integer.parseInt(portLine.trim());
            String[] bootstrapInfo = bootstrapLine.trim().split("\\s+");
            String bootstrapIP = bootstrapInfo[0];
            int bootstrapPort = Integer.parseInt(bootstrapInfo[1]);

            KeyValueStore keyValueStore = new KeyValueStore();
            KeyTransferService keyTransferService = new KeyTransferService(keyValueStore);

            NameServer nameServer = new NameServer(nodeId, localPort, bootstrapIP,
                    bootstrapPort, keyValueStore, keyTransferService);
            new Thread(nameServer::startServer).start();

            NameServerCLI cli = new NameServerCLI(nodeId, localPort, bootstrapIP,
                    bootstrapPort, keyValueStore, keyTransferService, nameServer);
            new Thread(cli::startCLI).start();

        } catch (IOException e) {
            System.out.println("Error reading config file: " + e.getMessage());
        } catch (NumberFormatException e) {
            System.out.println("Invalid configuration format: " + e.getMessage());
        }
    }
}
