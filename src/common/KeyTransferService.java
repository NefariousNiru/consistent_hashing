package common;

import java.io.PrintWriter;

public class KeyTransferService {
    private KeyValueStore keyValueStore;

    public KeyTransferService(KeyValueStore keyValueStore) {
        this.keyValueStore = keyValueStore;
    }

    public synchronized void sendKeyValueRange(PrintWriter out, Range range) {
        StringBuilder output = new StringBuilder();
        for (int key = range.getStart(); key <= range.getEnd(); key++) {
            String value = keyValueStore.lookup(key);
            if (value != null)
                output.append(key).append(":").append(value).append("%0A");
        }
        out.println(output+"FIN");
    }
}
