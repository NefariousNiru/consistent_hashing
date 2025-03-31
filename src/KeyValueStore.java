import java.util.Map;
import java.util.HashMap;

public class KeyValueStore {
    private final Map<Integer, String> keyValueStore;

    public KeyValueStore() {
        this.keyValueStore = new HashMap<>();
    }

    public synchronized int insert(int key, String value) {
        if(!keyValueStore.containsKey(key)){
            keyValueStore.put(key, value);
            return 0;
        }
        return -1;
    }

    public synchronized String lookup(int key) {
        return keyValueStore.get(key);
    }

    public synchronized int delete(int key) {
        if(keyValueStore.containsKey(key)){
            keyValueStore.remove(key);
            return 0;
        }
        return -1;
    }
}
