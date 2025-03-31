package util;

import java.util.TreeSet;

public class RangeManager {
    private final short MAXRANGE = 1023;
    private TreeSet<Integer> nodeIds;

    public RangeManager() {
        this.nodeIds = new TreeSet<Integer>();
        nodeIds.add(0); // Add the bootstrap node
    }

    public synchronized int addNode(int nodeId) {
        if (nodeId < 0 || nodeId > MAXRANGE) {
            System.out.println("Invalid node ID. Must be between 0 and " + MAXRANGE);
            return -1;
        }

        if (nodeIds.contains(nodeId)) {
            System.out.println("Node " + nodeId + " already exists.");
            return -1;
        }
        nodeIds.add(nodeId);
        System.out.println("Node " + nodeId + " added.");
        return 0;
    }

    public synchronized int removeNode(int nodeId) {
        if (nodeId < 0 || nodeId > MAXRANGE) {
            System.out.println("Invalid node ID. Must be between 0 and " + MAXRANGE);
            return -1;
        }

        if (nodeId == 0) {
            System.out.println("Bootstrap node cannot be removed.");
            return -1;
        }

        if (nodeIds.contains(nodeId)) {
            nodeIds.remove(nodeId);
            System.out.println("Node " + nodeId + " removed.");
            return 0;
        }

        System.out.println("Node " + nodeId + " does not exist.");
        return -1;
    }

    public synchronized String getRangeForNode(int nodeId) {
        if (nodeId > MAXRANGE) return null;
        if (!nodeIds.contains(nodeId)) return null;
        if (nodeId == 0) {
            if (nodeIds.size() == 1) {
                return "0-" + MAXRANGE;
            }
            int last = nodeIds.last();
            if (last == 0) return "0-" + MAXRANGE;

            // Assume node 1023 joins. then last would be 1023 + 1 = 1024 and MAXRANGE + 1 = 1024
            // Therefore 1024 % 1024 is = 0 so node 0's range is 0-0 (only key 0)
            int start = (last + 1) % (MAXRANGE + 1);
            return start + "-0";
        } else {
            // For any non-zero node their range is the previousValue (lower) + 1 to the nodeId
            Integer lower = nodeIds.lower(nodeId);
            if (lower == null) lower = 0;     // This will not execute as bootstrap is always in set but doesn't hurt to add
            int start = lower + 1;
            return start + "-" + nodeId;
        }
    }

    public synchronized void printRanges() {
        System.out.println("Current Node Ranges:");
        for (Integer id : nodeIds) {
            System.out.println("Node " + id + " range: " + getRangeForNode(id));
        }
    }
}
